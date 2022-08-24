#pragma once
#include <string>
#include <iostream>
#include <vector>
#include <algorithm>


enum class CSVState: int {
    ///there is next field at the row
    next = 0,
    ///this field is last field at the row
    last = 1,
    ///do more field, eof reached
    eof = std::char_traits<char>::eof()
};

///Describes mapping of column to a string field in the target structure
/**
 * @tparam T type of target structure
 */
template<typename T>
struct CSVFieldMapping {
    typedef std::string T::*FieldRef;
    ///name of field
    std::string name;
    ///pointer to member field
    std::string T::*field;
};

///Contains final mapping columns to structure. The instance is created by function mapColumns
template<typename T>
class  CSVFieldIndexMapping : public std::vector<typename CSVFieldMapping<T>::FieldRef > {
public:
    using std::vector<typename CSVFieldMapping<T>::FieldRef  >::vector;
    ///contains true, if all fields has been mapped. Otherwise it is false
    /** to find, which field is not mapped you need to find it manually
     */
    bool allMapped = false;
};

///Simple CSV reader
/**
 * Parses CSV from the source. Each read returns one field until whole CSV is parsed
 *
 * @tparam Source the type of object which provides source data. This can also be a function.
 * because the type must implement following function call <int()>. The input characters
 * should be read as unsigned (so characters above 0x80 are not mapped bellow the zero). The
 * function must return EOF (-1) as end of file.
 */
template<typename Source>
class CSVReader {
public:

    ///Inicialize parser
    /**
     * @param src data source
     */
    CSVReader(Source &&src)
        : _src(std::forward<Source>(src)) {}

    ///Read next field
    /**
     * @param buffer reference to a string, which will be filled with content of next field.
     *   The string is always cleared at the beginning regardless on how operation completes
     * @retval next - field read successfully and there is at least one further field
     *                   on the same row
     * @retval last - field read successfully and this was the last field on the row,
     *                   next read() will read a field on a new row
     * @retval eof - reached end of file, buffer is empty
     */
    CSVState read(std::string &buffer);

    ///Contains field separator. You can freely change this value to have different separator
    /** You should change it before you start reading */
    char _sep = ',';
    ///Contains quotes character. You can freely change this value to have different quotes
    /** You should change it before you start reading */
    char _quotes = '"';

    ///Contains eof value
    static constexpr auto eof = std::char_traits<char>::eof();


    ///Reads line and creates mapping of columns to structure
    /**
     * Function is intended to be called for the first line, which contains columns' names
     *
     * @tparam T You probably need to explicitly specify type
     * @param def definition
     * @return mapping
     *
     *
     * @code
     * struct CSVCols {
     *       std::string symbol_data_type;
     *       std::string format;
     *       std::string interval;
     *       std::string directory;
     *   };
     *
     * auto mapping = reader.mapColumns<CSVCols>({
     *       {"symbol_data_type",&CSVCols::symbol_data_type},
     *       {"format",          &CSVCols::format},
     *       {"interval",        &CSVCols::interval},
     *       {"directory",       &CSVCols::directory},
     *   });
     * if (!mapping.allMapped) return false;
     * @encode
     */
    template<typename T>
    CSVFieldIndexMapping<T> mapColumns(const std::initializer_list<CSVFieldMapping<T> > &def);


    ///Reads row and transfer data to target structure through the mapping object
    /**
     * @param mapping mapping object created by mapColumns
     * @param target target structure which is filled from the line
     * @retval true some data read
     * @retval false end of table (no data read)
     */
    template<typename T>
    bool readRow(const CSVFieldIndexMapping<T>mapping, T &target);

protected:
    Source _src;
    bool _eof_reached = false;
    bool _beg_line = true;

};

///Instantiate parser supplying a lambda function as a source
template<typename Fn>
CSVReader<Fn> parseCSV(Fn &&fn) {
    return CSVReader<Fn>(std::forward<Fn>(fn));
}

///Instantiate parse supplying an input stream as a source
/**
 * @param input input stream. Note it must stay valid during parsing
 * @return CSVReader instance
 */
inline auto parseCSVFromFile(std::istream &input) {
    return parseCSV([&]{return input.get();});
}

///Instantiate the parser supplying a string as a source
/**
 * @param str string source
 * @return CSVReader reader
 */
inline auto parseCSVString(std::string &&str) {
    return parseCSV([s = std::move(str), pos = std::size_t(0)]() mutable {
        return pos>=s.size()?std::char_traits<char>::eof()
                    :static_cast<int>(static_cast<unsigned char>(s[pos++]));
    });
}

///Instantiate the parser supplying a string as a source
/**
 * @param str string source - must be valid during parsing
 * @return CSVReader reader
 */
inline auto parseCSVString(const std::string &str) {
    return parseCSV([&, pos = std::size_t(0)]() mutable {
        return pos>=str.size()?std::char_traits<char>::eof()
                    :static_cast<int>(static_cast<unsigned char>(str[pos++]));
    });
}

template<typename Source>
inline CSVState CSVReader<Source>::read(std::string &buffer) {
    buffer.clear();
    if (_eof_reached) return CSVState::eof;
    int c = _src();
    if (_beg_line) {
        while (c != eof && std::iscntrl(c)) c = _src();
    }
    if (c == eof) return CSVState::eof;
    if (c == _quotes) {
        bool loop;
        int c= _src();
        do {
            while (c != _quotes && c != eof) {
                buffer.push_back(static_cast<char>(c));
                c = _src();
            }
            loop = false;
            if (c == _quotes) {
                c = _src();
                if (c == _quotes) {
                    loop = true;
                    buffer.push_back(_quotes);
                }
            }
        } while (loop);
        while (c != eof && c != _sep && c != '\n' && c != '\r') {
            c = _src(); //ignore incorrect csv content
        }
    } else {
        while (c != _sep && c != eof && c != '\n' && c != '\r') {
            buffer.push_back(static_cast<char>(c));
            c = _src();
        }
    }


    if (c == eof) _eof_reached = true;
    _beg_line =  c != _sep;

    return _beg_line?CSVState::last:CSVState::next;
}

template<typename Source>
template<typename T>
inline CSVFieldIndexMapping<T> CSVReader<Source>::mapColumns(const std::initializer_list<CSVFieldMapping<T> > &def) {
    CSVFieldIndexMapping<T> out;
    std::string buff;
    std::size_t cnt = def.size();
    CSVState st = CSVState::next;
    while (st != CSVState::last) {
        st = this->read(buff);
        if (st == CSVState::eof) break;
        auto iter = std::find_if(def.begin(), def.end(), [&](const CSVFieldMapping<T> &x){
            return x.name == buff;
        });
        if (iter != def.end()) {
            out.push_back(iter->field);
            cnt--;
        } else {
            out.push_back(nullptr);
        }
    }
    out.allMapped = cnt == 0;
    return out;
}

template<typename Source>
template<typename T>
inline bool CSVReader<Source>::readRow(const CSVFieldIndexMapping<T> mapping,T &target) {
    std::string buff;
    std::size_t idx = 0;
    CSVState st = CSVState::next;
    while (st == CSVState::next) {
        if (idx < mapping.size()) {
            const auto &m = mapping[idx];
            if (m){
                st = read(target.*m);
            } else {
                st = read(buff);
            }
            if (st == CSVState::eof)
                break;
            ++idx;
        } else {
            st = read(buff);
        }
    }
    return st != CSVState::eof || idx != 0;
}

