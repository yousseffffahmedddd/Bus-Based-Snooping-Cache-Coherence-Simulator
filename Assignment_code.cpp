// ============================================================
// Bus-Based Snooping Cache Coherence Simulator  ( - dynamic)
// Protocols: 1) Write-Invalidate Write-Through
//            2) Write-Invalidate Write-Back (Ownership)
//            3) Goodman's Write-Once
//
// : any number of processors, any number of variables,
//     any access stream (all read from the input file).
// ============================================================
#include <iostream>
#include <iomanip>      // setw / left / right for aligned table
#include <map>
#include <fstream>
#include <sstream>
#include <vector>
using namespace std;

// All possible cache block states across the 3 protocols
enum STATES{INV, VALID, RO, RW, RES, DIRTY};

// Convert a state to text for printing
string state_name(STATES s) {
    switch (s) {
        case INV:   return "INV";
        case VALID: return "VALID";
        case RO:    return "RO";
        case RW:    return "RW";
        case RES:   return "RES";
        case DIRTY: return "DIRTY";
    }
    return "-";
}

// : remove spaces, tabs and '\r' from both ends of a string
// (makes the parser safe for Windows line endings and extra spaces)
string trim(const string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

// Kinds of messages that travel on the shared bus
enum BusTx { BUS_READ, BUS_INVALIDATE, BUS_UPDATE };

// What a cache answers when it snoops a bus transaction
struct SnoopResult {
    bool hit = false;         // a valid copy was found in this cache
    bool supplied = false;    // this cache supplied the data
    bool wrote_back = false;  // owner flushed its value to memory
    bool was_written = false; // block state was RES or DIRTY
    int  value = 0;           // value held by the snooped copy
    int  count = 0;           // number of copies affected
};

// ------------------------------------------------------------
// One cache block = one variable
// ------------------------------------------------------------
class CacheBlock{
    public:
     string tag; //variable name
     int value; 
     STATES state;
    CacheBlock() {
        tag = "";
        value = 0;  
        state = INV;
    }
     CacheBlock(string t,int v,STATES s){
        tag=t;
        value=v;
        state=s;
     }
};

// ------------------------------------------------------------
// Private cache of one processor
// ------------------------------------------------------------
class Cache{
   public:
    string processor_id;
    map<string,CacheBlock>blocks;   // variable name -> block
     Cache(string p_id){
        processor_id=p_id;
        
    }
    
   // Read hit only if block exists and is not INV
   pair<bool,int> read(string var ){

    if(blocks.count(var)>0 && blocks[var].state !=STATES::INV){
        return{true,blocks[var].value};
    }
    else{
        return{false,0};
    }
    
   }

   // Write locally (only if the block is valid)
   bool write(string var,int newvalue){
     if(blocks.count(var)>0 && blocks[var].state !=STATES::INV){
        blocks[var].value=newvalue;
        return true;
     }
     return false;
   }

   // Mark a block invalid
   void invalidate(string var){
    if(blocks.count(var)>0){blocks[var].state=STATES::INV;}
   }

   void store(string var,int value,STATES s) //new cacheblock or replace cacheblock in processor 
   {
    blocks[var]=CacheBlock(var,value,s);
   }

   // snoop() - the cache reacts to bus traffic by itself.
   // The bus only broadcasts; the state changes happen here.
   SnoopResult snoop(BusTx tx, const string& var,
                     int protocol, int new_value = 0) {
       SnoopResult r;
       auto it = blocks.find(var);

       // No valid copy here -> nothing to do
       if (it == blocks.end() || it->second.state == INV) return r;

       CacheBlock& b = it->second;
       r.hit = true;
       r.value = b.value;
       r.was_written = (b.state == RES || b.state == DIRTY);

       if (tx == BUS_READ) {
           // Protocol 2: RW owner supplies data, flushes memory, RW -> RO
           if (protocol == 2 && b.state == RW) {
               r.supplied = r.wrote_back = true;
               b.state = RO;
           }
           // Protocol 3: DIRTY owner supplies data, flushes memory, -> VALID
           else if (protocol == 3 && b.state == DIRTY) {
               r.supplied = r.wrote_back = true;
               b.state = VALID;
           }
           // Protocol 3: RES owner -> VALID (memory is already correct)
           else if (protocol == 3 && b.state == RES) {
               b.state = VALID;
           }
       }
       // Another cache wrote this variable -> my copy becomes INV
       else if (tx == BUS_INVALIDATE) b.state = INV;
       // Write-update (bonus protocols): take the new value
       else if (tx == BUS_UPDATE)     b.value = new_value;

       return r;
   }

};

// ------------------------------------------------------------
// Global shared memory
// ------------------------------------------------------------
class MainMemory{
    public:
   map<string,int>storage;
   int read(string var){
    return storage[var];
   }
   void write(string var,int newvalue){
    storage[var]=newvalue;
   }
};

// ------------------------------------------------------------
// SharedBus - only broadcasts to all caches (via snoop)
// and collects replies. It never edits cache internals.
// : it points to the simulator's cache vector, so it works
//     with any number of processors (no fixed 3 caches).
// ------------------------------------------------------------
class SharedBus{
    public:
    vector<Cache>* caches;       // : all caches (size = #processors)
    MainMemory * globalmemory;

    SharedBus(vector<Cache>* c, MainMemory* mem) {
        caches = c;
        globalmemory = mem;
    }

    // Read miss on the bus: ask every other cache to snoop.
    // If an owner flushes, the bus writes the value to memory.
    SnoopResult broadcast_read(const string& var, int requester, int protocol) {
        SnoopResult out;
        for (int i = 0; i < (int)caches->size(); i++) {
            if (i == requester) continue;

            SnoopResult r = (*caches)[i].snoop(BUS_READ, var, protocol);
            if (!r.hit) continue;

            // Prefer the value from the cache that supplied the data
            if (!out.hit || r.supplied) out.value = r.value;
            out.hit = true;

            if (r.wrote_back) {
                write_back_memory(var, r.value);
                out.wrote_back = true;
            }
        }
        return out;
    }

    // Invalidate command: every other copy becomes INV.
    // count = number of copies invalidated.
    SnoopResult broadcast_invalidate(const string& var, int sender) {
        SnoopResult out;
        for (int i = 0; i < (int)caches->size(); i++) {
            if (i == sender) continue;

            SnoopResult r = (*caches)[i].snoop(BUS_INVALIDATE, var, 0);
            if (!r.hit) continue;

            out.hit = true;
            out.count++;
            if (r.was_written) out.was_written = true;
        }
        return out;
    }

    // Update broadcast: reserved for the bonus write-update protocols
    SnoopResult broadcast_update(const string& var, int sender, int value) {
        SnoopResult out;
        for (int i = 0; i < (int)caches->size(); i++) {
            if (i == sender) continue;

            SnoopResult r = (*caches)[i].snoop(BUS_UPDATE, var, 0, value);
            if (r.hit) { out.hit = true; out.count++; }
        }
        return out;
    }

    // Write a value back to main memory
    void write_back_memory(const string& var, int value) {
        globalmemory->write(var, value);
    }
};

// ------------------------------------------------------------
// Controller / engine: loads input, runs operations, prints output
// ------------------------------------------------------------
class CoherenceSimulator {
 
    int protocol1=1; //write invalidate write through
    int protocol2=2; // write invalidate write back 
    int protocol3=3; //write once
    int protocol=1;  // protocol currently being simulated
 string TestCase1 = "C:\\Users\\Lenovo\\Desktop\\New folder\\Fall26\\HPC\\Shared_Memory_Assignement\\TestCase1.txt";
string TestCase2 = "C:\\Users\\Lenovo\\Desktop\\New folder\\Fall26\\HPC\\Shared_Memory_Assignement\\TestCase2.txt";
    MainMemory memory; //global memory
      SharedBus bus;    //bus 
      vector<Cache> caches;   // : one cache per processor (built in load_input)

    // One row of the trace table = a snapshot after one operation
    struct TraceRow {
        int serial;
        string event;
        string operation;   // Read-Hit / Read-Miss / Write-Hit / Write-Miss / Replace

        map<string, int> memory_snapshot;

        vector<map<string, CacheBlock>> cache_snapshot;
    };

    vector<TraceRow> trace;
    public:

    CoherenceSimulator()
    : bus(&caches, &memory) {}
  
    // One parsed operation line, e.g.  P: Write(X, 10)
    struct Operation {
        string processor;
        string type;       // Read, Write, Replace
        string variable;
        int value = 0;     // Used by Write
    };
    vector<Operation> operations;
    vector<string> active_processors;

    // Statistics
    int read_hits = 0;
    int read_misses = 0;
    int write_hits = 0;
    int write_misses = 0;
    int invalidations = 0;
    int write_backs = 0;

   // Clear everything before a new run
   void reset_simulator() {

        trace.clear();

        read_hits = 0;
        read_misses = 0;

        write_hits = 0;
        write_misses = 0;

        invalidations = 0;
        write_backs = 0;

        for (size_t i = 0; i < caches.size(); i++) {
            caches[i].blocks.clear();
        }

        memory.storage.clear();
    }

    // Save memory + all caches as one trace row
    void save_trace(int serial, string event, string operation = "-") {

        TraceRow row;

        row.serial = serial;
        row.event = event;
        row.operation = operation;

        row.memory_snapshot = memory.storage;

        for (size_t i = 0; i < caches.size(); i++) {

            row.cache_snapshot.push_back(
                caches[i].blocks
            );
        }

        trace.push_back(row);
    }

    // ========================================================
    // Trace table aligned with setw
    // : column widths are computed from the data, so long
    //     names / big values / many processors still align.
    // ========================================================
    void print_trace_table() {

        // Variable names in memory order (X, Y, Z ...)
        vector<string> vars;
        for (auto& item : memory.storage) vars.push_back(item.first);

        size_t n = active_processors.size();

        // Text of one cache cell: "value STATE" or "- -" if absent
        auto cell_of = [](const map<string, CacheBlock>& blocks,
                          const string& v) {
            if (blocks.count(v))
                return to_string(blocks.at(v).value) + " "
                     + state_name(blocks.at(v).state);
            return string("- -");
        };

        // ---- Compute column widths from the data ----
        int W_SER = 6;                         // "Serial"
        int W_EVT = 5;                         // "Event"
        int W_OP  = 9;                         // "Operation"
        int W_MEM = 1;
        int W_CEL = 1;

        for (auto& v : vars) {
            W_MEM = max(W_MEM, (int)v.size());
            W_CEL = max(W_CEL, (int)(string("Val/St(") + v + ")").size());
        }
        for (const auto& row : trace) {
            W_SER = max(W_SER, (int)to_string(row.serial).size());
            W_EVT = max(W_EVT, (int)row.event.size());
            W_OP  = max(W_OP,  (int)row.operation.size());
            for (auto& v : vars) {
                W_MEM = max(W_MEM,
                    (int)to_string(row.memory_snapshot.at(v)).size());
                for (size_t i = 0; i < n; i++)
                    W_CEL = max(W_CEL,
                        (int)cell_of(row.cache_snapshot[i], v).size());
            }
        }
        W_MEM += 1;                            // gap between columns
        W_CEL += 1;

        // Group widths: at least as wide as the header text above them
        int memW   = max((int)vars.size() * W_MEM, 6);          // "Memory"
        int cacheW = vars.size() * W_CEL;
        for (auto& p : active_processors)
            cacheW = max(cacheW, (int)(p + " Cache").size());   // "P Cache"
        int total  = W_SER + 3 + W_EVT + 3 + W_OP + 3 + memW + n * (3 + cacheW);
        string line(total, '=');

        // Join cells (each W wide) into one string for a whole group
        auto join = [](const vector<string>& cells, int w) {
            ostringstream o;
            o << left;
            for (auto& c : cells) o << setw(w) << c;
            return o.str();
        };

        cout << left << "\n" << line << "\n";

        // ---- Header row 1: column group names ----
        cout << setw(W_SER) << "Serial" << " | "
             << setw(W_EVT) << "Event"  << " | "
             << setw(W_OP)  << "Operation" << " | "
             << setw(memW)  << "Memory";
        for (auto& p : active_processors)
            cout << " | " << setw(cacheW) << (p + " Cache");
        cout << "\n";

        // ---- Header row 2: variable names under each group ----
        vector<string> memHdr, celHdr;
        for (auto& v : vars) {
            memHdr.push_back(v);
            celHdr.push_back("Val/St(" + v + ")");
        }
        cout << setw(W_SER) << "" << " | " << setw(W_EVT) << "" << " | "
             << setw(W_OP) << "" << " | "
             << setw(memW) << join(memHdr, W_MEM);
        for (size_t i = 0; i < n; i++)
            cout << " | " << setw(cacheW) << join(celHdr, W_CEL);
        cout << "\n" << line << "\n";

        // ---- Data rows: one per saved snapshot ----
        for (const auto& row : trace) {
            cout << setw(W_SER) << row.serial << " | "
                 << setw(W_EVT) << row.event  << " | "
                 << setw(W_OP)  << row.operation << " | ";

            // Memory values
            vector<string> memCells;
            for (auto& v : vars)
                memCells.push_back(to_string(row.memory_snapshot.at(v)));
            cout << setw(memW) << join(memCells, W_MEM);

            // Each processor's cache
            for (size_t i = 0; i < n; i++) {
                vector<string> cells;
                for (auto& v : vars)
                    cells.push_back(cell_of(row.cache_snapshot[i], v));
                cout << " | " << setw(cacheW) << join(cells, W_CEL);
            }
            cout << "\n";
        }
        cout << line << "\n" << right;   // restore default alignment
    }

    // ========================================================
    //  14: Final system summary and statistics
    // ========================================================

    void print_summary() {

        cout << "\n========== FINAL SYSTEM SUMMARY ==========\n";

        cout << "\nMain Memory:\n";

        for (auto item : memory.storage) {

            cout << item.first
                 << " = "
                 << item.second << "\n";
        }

        for (size_t i = 0;
             i < active_processors.size();
             i++) {

            cout << "\n"
                 << active_processors[i]
                 << " Cache:\n";

            if (caches[i].blocks.empty()) {
                cout << "Empty\n";
            }

            for (auto item : caches[i].blocks) {

                cout << item.first
                     << " = "
                     << item.second.value
                     << " | "
                     << state_name(item.second.state)
                     << "\n";
            }
        }

        cout << "\n========== EXECUTION STATISTICS ==========\n";

        cout << "Read Hits       : " << read_hits << "\n";
        cout << "Read Misses     : " << read_misses << "\n";

        cout << "Write Hits      : " << write_hits << "\n";
        cout << "Write Misses    : " << write_misses << "\n";

        cout << "Invalidations   : " << invalidations << "\n";
        cout << "Memory Writebacks: " << write_backs << "\n";

        cout << "==========================================\n";
    }

    // ========================================================
    // : Read the input file (any number of processors,
    //     variables and operations).
    //   - blank lines and '#' comment lines are skipped
    //   - spaces / Windows line endings are tolerated
    //   - PROTOCOL: line is accepted and ignored (menu picks it)
    //   - processors missing from PROCESSORS: are taken from the stream
    //   - variables missing from MEMORY: are added with value 0
    // ========================================================
    void load_input(string filename) {
    // Reset everything before loading
    operations.clear();
    memory.storage.clear();
    active_processors.clear();
    trace.clear();
    caches.clear();                  // : caches are rebuilt below

    read_hits = 0;
    read_misses = 0;
    write_hits = 0;
    write_misses = 0;
    invalidations = 0;
    write_backs = 0;

    ifstream file(filename);
    if (!file.is_open()) {
    cout << "Cannot open file: " << filename << endl;
    return;
}
    string line;

    while (getline(file, line)) {

        line = trim(line);
        if (line.empty() || line[0] == '#') continue;   // skip blanks/comments

        // ---- PROTOCOL: ... (chosen from the menu instead) ----
        if (line.find("PROTOCOL:") == 0) {
            continue;
        }

        // ---- MEMORY: X=5, Y=10, ... (any number of variables) ----
        else if (line.find("MEMORY:") == 0) {

            string data = line.substr(7);
            stringstream ss(data);
            string item;

            while (getline(ss, item, ',')) {
                size_t eq = item.find('=');
                if (eq == string::npos) {
                    cout << "Invalid memory item: " << item << endl;
                    continue;
                }
                string var = trim(item.substr(0, eq));
                try {
                    memory.storage[var] = stoi(trim(item.substr(eq + 1)));
                } catch (...) {
                    cout << "Invalid memory value: " << item << endl;
                }
            }
        }

        // ---- PROCESSORS: P, Q, R, ... (any number of processors) ----
        else if (line.find("PROCESSORS:") == 0) {
        active_processors.clear();

        string data = line.substr(11);
        stringstream ss(data);
        string processor;

        while (getline(ss, processor, ',')) {
            processor = trim(processor);
            if (processor.empty()) continue;
            // ignore duplicates
            bool seen = false;
            for (auto& p : active_processors) if (p == processor) seen = true;
            if (!seen) active_processors.push_back(processor);
        }
        }

      // ---- Operation line: P: Read(X) / Write(X, 10) / Replace(X) ----
      else if (line.find(':') != string::npos) {

    Operation op;

    size_t colon = line.find(':');
    size_t open = line.find('(');
    size_t close = line.find(')');

    if (open == string::npos || close == string::npos ||
        close < open || colon > open) {
        cout << "Invalid operation line: " << line << endl;
        continue;
    }

    op.processor = trim(line.substr(0, colon));
    op.type = trim(line.substr(colon + 1, open - colon - 1));

    if (op.type == "Read" || op.type == "Replace") {
        op.variable = trim(line.substr(open + 1, close - open - 1));
    }
    else if (op.type == "Write") {

        size_t comma = line.find(',', open);

        if (comma == string::npos || comma > close) {
            cout << "Invalid Write operation: " << line << endl;
            continue;
        }

        op.variable = trim(line.substr(open + 1, comma - open - 1));
        try {
            op.value = stoi(trim(line.substr(comma + 1, close - comma - 1)));
        } catch (...) {
            cout << "Invalid Write value: " << line << endl;
            continue;
        }
    }
    else {
        cout << "Unknown operation: " << op.type << endl;
        continue;
    }

    if (op.processor.empty() || op.variable.empty()) {
        cout << "Invalid operation line: " << line << endl;
        continue;
    }

    operations.push_back(op);
}
    }

    file.close();

    // : no PROCESSORS: line -> take processors from the stream (first-seen order)
    if (active_processors.empty()) {
        for (auto& op : operations) {
            bool seen = false;
            for (auto& p : active_processors) if (p == op.processor) seen = true;
            if (!seen) active_processors.push_back(op.processor);
        }
    }

    // : a variable used in the stream but not in MEMORY: starts at 0
    for (auto& op : operations) {
        if (!memory.storage.count(op.variable)) {
            cout << "Warning: variable " << op.variable
                 << " not in MEMORY, starting at 0\n";
            memory.storage[op.variable] = 0;
        }
    }

    // : build one cache per processor
    for (auto& p : active_processors)
        caches.push_back(Cache(p));

    save_trace(0, "Initial State"); // Save initial state
}

// ============================================================
// run_all() split into small functions
//   do_read / do_write / do_replace  (one job each)
// ============================================================

// Map a processor name to its cache index using the PROCESSORS line
// (e.g. P,Q,R -> 0,1,2). Returns -1 if the name is unknown.
int processor_index(const string& name) {
    for (size_t i = 0; i < active_processors.size(); i++)
        if (active_processors[i] == name) return i;
    return -1;
}

// READ: hit -> local read. Miss -> ask the bus, then fill the cache.
string do_read(int pid, const string& var) {
    Cache& c = caches[pid];

    // Read hit: no bus traffic
    if (c.read(var).first) { read_hits++; return "Read-Hit"; }
    read_misses++;

    // Read miss: broadcast on the bus (owner may supply + flush memory)
    SnoopResult s = bus.broadcast_read(var, pid, protocol);
    if (s.wrote_back) write_backs++;

    // Data comes from another cache if one had it, else from memory
    int v = s.hit ? s.value : memory.read(var);

    // Protocol 2 loads as RO, protocols 1 and 3 load as VALID
    c.store(var, v, protocol == 2 ? RO : VALID);
    return "Read-Miss";
}

// WRITE: behavior depends on the selected protocol
string do_write(int pid, const string& var, int val) {
    Cache& c = caches[pid];

    bool hit = c.read(var).first;
    hit ? write_hits++ : write_misses++;
    STATES cur = hit ? c.blocks[var].state : INV;

    // ---- Protocol 1: write-invalidate + write-through ----
    if (protocol == 1) {
        invalidations += bus.broadcast_invalidate(var, pid).count;
        bus.write_back_memory(var, val);   // memory always updated
        c.store(var, val, VALID);
    }
    // ---- Protocol 2: write-invalidate + write-back (ownership) ----
    else if (protocol == 2) {
        if (hit && cur == RW) {
            c.write(var, val);             // RW hit: no bus traffic
        } else {
            invalidations += bus.broadcast_invalidate(var, pid).count;
            c.store(var, val, RW);         // take exclusive ownership
        }
    }
    // ---- Protocol 3: Goodman's write-once ----
    else {
        SnoopResult r = bus.broadcast_invalidate(var, pid);
        invalidations += r.count;

        // Subsequent write = another cache already RES/DIRTY,
        // or my own copy is RES/DIRTY
        bool subsequent = r.was_written ||
                          (hit && (cur == RES || cur == DIRTY));
        if (subsequent) {
            c.store(var, val, DIRTY);      // write-back: memory stale
        } else {
            bus.write_back_memory(var, val); // first write: write-through
            c.store(var, val, RES);
        }
    }
    return hit ? "Write-Hit" : "Write-Miss";
}

// REPLACE: evict a block; write back only when memory would be stale
string do_replace(int pid, const string& var) {
    Cache& c = caches[pid];

    auto it = c.blocks.find(var);
    if (it == c.blocks.end()) return "Replace-Miss";   // block not in cache

    STATES s = it->second.state;

    // Protocol 2: RW needs write-back. Protocol 3: DIRTY needs write-back.
    if ((protocol == 2 && s == RW) || (protocol == 3 && s == DIRTY)) {
        bus.write_back_memory(var, it->second.value);
        write_backs++;
    }
    c.blocks.erase(it);
    return "Replace";
}

// Controller loop: run every operation, then save a trace row
void run_all(int proto) {
    protocol = proto;
    int serial = 0;

    for (const Operation& op : operations) {

        int pid = processor_index(op.processor);
        if (pid < 0) {
            cout << "Unknown processor: " << op.processor << endl;
            continue;
        }

        string result = "-";   // Read-Hit, Read-Miss, Write-Hit, Write-Miss, Replace
        if      (op.type == "Read")    result = do_read(pid, op.variable);
        else if (op.type == "Write")   result = do_write(pid, op.variable, op.value);
        else if (op.type == "Replace") result = do_replace(pid, op.variable);

        // Save state after every operation
        save_trace(++serial,
                   op.processor + " " + op.type + "(" + op.variable + ")",
                   result);
    }
}

    // Menu: choose a protocol, run both test cases
    void run(){
        int choosen=0;
        cout<<"Choose Protocol \nWRITE_INVALIDATE_WRITE_THROUGH (1) \n| WRITE_INVALIDATE_WRITE_BACK (2) \n|WRITE_ONCE (3)\n";
        cin>>choosen;
        if(choosen==protocol1){

        cout<<"######### Test case 1 ############\n";
        load_input(TestCase1);
        run_all(protocol1);
        print_trace_table();
        print_summary();
        cout<<"######### Test case 2 ############\n";
        load_input(TestCase2);
        run_all(protocol1);
        print_trace_table();
        print_summary();

        }
        if(choosen==protocol2){
        cout<<"######### Test case 1 ############\n";
        load_input(TestCase1);
        run_all(protocol2);
        print_trace_table();
        print_summary();
        cout<<"######### Test case 2 ############\n";
        load_input(TestCase2);
        run_all(protocol2);
        print_trace_table();
        print_summary();
        }

        if(choosen==protocol3){
        cout<<"######### Test case 1 ############\n";
        load_input(TestCase1);
        run_all(protocol3);
        print_trace_table();
        print_summary();
        cout<<"######### Test case 2 ############\n";
        load_input(TestCase2);
        run_all(protocol3);
        print_trace_table();
        print_summary();
    }
}
};

int main() {
    CoherenceSimulator simulator;
    simulator.run();
    return 0;
}