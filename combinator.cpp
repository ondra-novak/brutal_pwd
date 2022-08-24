#include "CSVReader.h"
#include "BloomSet.h"
#include "ThreadPool.h"
#include <fstream>
#include <algorithm>
#include <numeric>
#include <unordered_set>
#include <deque>
#include <future>


struct WordDef {
    std::string w;
    unsigned char max_comb;
};

using WordList = std::vector<WordDef>;
using WordSizeMap = std::vector<WordList>;


WordSizeMap word_db;
WordList word_list;
bool verbose = false;

struct CSVLine {
    std::string word;
    std::string max_count;
};



void addWord(std::string &&word, std::size_t count) {
    unsigned char cnt;
    if (count > 255) cnt = 0; else cnt= static_cast<unsigned char>(count);
    word_list.push_back(WordDef{word, cnt});
    if (std::isalnum(word[0])) {
        char c;
        if (std::isupper(word[0])) {
            c = std::tolower(word[0]);
        } else {
            c = std::toupper(word[0]);
        }
        if (c != word[0]) {
            word[0] = c;
            word_list.push_back(WordDef{word, cnt});
        }
    }

}

void loadWordsText(std::istream &input, std::size_t defcount) {
    std::string buffer;
    while (!!input) {
        std::getline(input, buffer);
        if (!buffer.empty()) {
            addWord(std::move(buffer), defcount);
        }
    }
}

void loadWordsCSV(std::istream &input, std::size_t defcount) {
    auto csv = parseCSVFromFile(input);
    CSVLine ln;
    CSVFieldIndexMapping<CSVLine> mapping = {&CSVLine::word, &CSVLine::max_count};
    while (csv.readRow(mapping, ln)) {
        if (!ln.word.empty()) {
            unsigned int count = ln.max_count.empty()?defcount:std::strtoul(ln.max_count.c_str(),nullptr,10);
            addWord(std::move(ln.word), count);
        }
    }
}


template<typename Fn>
class Printer {
public:
    Printer(Fn &&fn):fn(std::move(fn)) {}
    void operator()(const std::vector<unsigned long> &selection);
    ~Printer() {
        if (!buffer.empty())
            fn(buffer);
        total_generated+=generated;
    }
    static std::atomic<std::size_t> total_generated;
protected:
    std::string buffer;
    Fn fn;
    std::size_t generated = 0;

};

template<typename Fn>
std::atomic<std::size_t> Printer<Fn>::total_generated (0);

template<typename Fn>
void Printer<Fn>::operator()(const std::vector<unsigned long> &selection) {
    for (const auto &x: selection) {
        buffer.append(word_list[x].w);
    }
/*    if (buffer.compare(buffer.size()-6,6,"hdhdhd") == 0) {
        std::cerr << "HIT";
    }*/
    buffer.append("\n");
    if (buffer.size()>128*1024) {
        fn(buffer);
        buffer.clear();
    }
    ++generated;

}

void dedupList() {
    std::sort(word_list.begin(),word_list.end(),[](const WordDef &a, const WordDef &b){
       return a.w < b.w;
    });
    auto iter = word_list.begin();
    auto beg = iter+1;
    auto end = std::remove_if(beg, word_list.end(), [&](const WordDef &a) {
      bool r = (a.w == iter->w);
      if (!r) ++iter;
      else {
          iter->max_comb = std::max(iter->max_comb, a.max_comb);
      }
      return r;
    });
    word_list.erase(end, word_list.end());
}

static inline std::size_t getLength(const WordDef &w) {return w.w.length();}
static inline std::size_t getLength(std::size_t s) {return s;}

template<typename Fn>
std::size_t run_cycle(std::vector<unsigned long> &selected,
        std::vector<unsigned char> &level,
        std::size_t min_chars, std::size_t max_chars,
        std::size_t max_level, std::size_t cur_count,
        Fn &fn) {

    std::size_t gen_cnt = 0;
    std::size_t end = word_list.size();
    if (selected.size()<max_level) {
        auto remain_levels = max_level-selected.size();
        if (cur_count < min_chars) { //test whether is worth to run this level
            auto remain = min_chars - cur_count;
            auto avail_size = (remain_levels+1) * word_list.back().w.length();
            if (avail_size < remain) return 0;
            //at least last word could be result, so continue
        }
        std::size_t fwsize = word_list[0].w.length();
        std::size_t remain_alloc = remain_levels * fwsize ;
        for(std::size_t i=0; i<end; ++i) {
            std::size_t newcount = cur_count + word_list[i].w.length();
            if (remain_alloc + newcount > max_chars) break;
            if ((!word_list[i].max_comb) || (level[i] < word_list[i].max_comb)) {
                selected.push_back(i);
                ++level[i];
                gen_cnt+=run_cycle(selected, level, min_chars, max_chars, max_level,newcount, fn);
                --level[i];
                selected.pop_back();
            }
        }
    } else {
        std::size_t start = 0;
        if (cur_count < min_chars) { //find matching word
            auto remain = min_chars - cur_count;
            auto iter = std::upper_bound(word_list.begin(), word_list.end(), remain-1,
                                         [](const auto &a, const auto &b){
                return getLength(a) < getLength(b);
            });
            start = std::distance(word_list.begin(), iter);
            //at least last word could be result, so continue
        }
        for(std::size_t i=start; i<end; ++i) {
            std::size_t newcount = cur_count + word_list[i].w.length();
            if (newcount > max_chars) break;
            if ((!word_list[i].max_comb) || (level[i] < word_list[i].max_comb)) {
                selected.push_back(i);
                ++level[i];
                fn(selected);
                --level[i];
                selected.pop_back();
                gen_cnt++;
            }
        }
    }
    return gen_cnt;
}

static std::mutex cerr_lock;

template<typename Fn>
void run_combinations(thread_pool &tp, std::size_t min_chars, std::size_t max_chars, std::size_t max_level, Fn &&fn) {
    std::size_t c = word_list.size();
    for (std::size_t s = 0; s < c; ++s) {
        tp >> [=]() mutable {
            if (verbose) {
                std::lock_guard<std::mutex> _(cerr_lock);
                std::cerr << "Start level:" << max_level << ", word: " << word_list[s].w << std::endl;
            }
            std::size_t count = 0;
            std::vector<unsigned long> selected;
            selected.push_back(s);
            std::vector<unsigned char> counters(word_list.size(),0);
            counters[s] = 1;
            if (max_level == 0) {
                auto sz = word_list[s].w.length();
                if (sz >=min_chars && sz <= max_chars) {
                        fn(selected);
                        count++;
                }
            } else {
                count = run_cycle(selected, counters, min_chars, max_chars, max_level, word_list[s].w.length(), fn);
            }
            if (verbose) {
                std::lock_guard<std::mutex> _(cerr_lock);
                std::cerr << "Finish level:" << max_level << ", word: " << word_list[s].w << ", generated: " << count << std::endl;
            }
        };
    }
}

void help(const char *arg0) {
    std::cerr << "Usage:" << std::endl;
    std::cerr << std::endl;
    std::cerr << "\t" << arg0 << " [...options...] <wordlist> [<wordlist2> ...]" << std::endl;
    std::cerr << std::endl;
    std::cerr << R"help(

<wordlist>      File cointains list of words. It can be text file where every word
                is on single line. It can also be CSV file, where two columns
                are supported. <word>,<max_frequency> - There you can specify
                how many times the word can repeat in a password. Default format
                is text, you can switch to csv by --csv 

--csv           Next wordlist will be parsed as CSV
--text          Next wordlist will be parsed as TEXT 
-1              Next wordlist will have default max frequency 1
-2              Next wordlist will have default max frequency 2
-3              Next wordlist will have default max frequency 3
-u              Next wordlist will have no limit on max frequency
-v              Verbose mode

--threads=nn    Specify count of threads (default: all available)
--pw-min=nn     Pasword minimal size (default 5)
--pw-max=nn     Pasword maximal size (default 20)
)help";
    exit(1);
}

int main(int argc, char **argv) {

    if (argc < 2) help(argv[0]);

    std::size_t minchars = 5;
    std::size_t maxchars = 20;
    std::size_t defcount = 3;
    std::size_t threads = 0;
    bool readCSV = false;

    for (int i = 1; i < argc; i++) {
        std::string_view a(argv[i]);
        if (a.substr(0,9) == "--pw-min=") minchars = std::strtoull(argv[i]+9,0,10);
        else if (a.substr(0,9) == "--pw-max=") maxchars = std::strtoull(argv[i]+9,0,10);
        else if (a.substr(0,10) == "--threads=") threads = std::strtoull(argv[i]+10,0,10);
        else if (a == "-1") defcount = 1;
        else if (a == "-2") defcount = 2;
        else if (a == "-3") defcount = 3;
        else if (a == "-u") defcount = 0;
        else if (a == "-v") verbose = true;
        else if (a == "-?" || a == "--help") help(argv[0]);
        else if (a == "--csv") readCSV = true;
        else if (a == "--text") readCSV = false;
        else if (a.substr(0,1) == "-") {
           std::cerr << "Invalid switch '" << a <<"'. See help --help" << std::endl;
           return 1;
        }
        else {
            std::ifstream stream(argv[i]);
            if (!stream) std::cerr << "Can't open wordlist: " << a << " - skipped" << std::endl;
            if (readCSV) loadWordsCSV(stream, defcount); else loadWordsText(stream, defcount);
        }
    }

    dedupList();
    std::sort(word_list.begin(), word_list.end(), [](const WordDef &a, const WordDef &b) {
       return a.w.length() < b.w.length();
    });


    std::mutex mx;
    auto printer = [&](const std::string &str) {
        std::lock_guard<std::mutex> _(mx);
        std::cout << str;
    };

    using MyPrinter = Printer<decltype(printer)>;

    if (!threads) threads = std::thread::hardware_concurrency();;
    if (!threads) {
        std::cerr << "Can't detect count of available CPUs. Running in 1 thread. Use --threads= to specify count of threads to use" << std::endl;
        threads = 1;
    }
    thread_pool thr(threads);
    MyPrinter out(std::move(printer));

    for (std::size_t i = 0; i < maxchars; i++) {
        run_combinations(thr, minchars, maxchars, i, out);
    }
    std::promise<void> p;
    thr >> [&]{p.set_value();};
    p.get_future().wait();
    thr.stop();
    std::cerr << "Total generated passwords: " << MyPrinter::total_generated;

}
