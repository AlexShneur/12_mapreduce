/**
 * Это MapReduce фреймворк.
 * Он универсальный.
 * Он может выполнять разные map_reduce задачи.
 * Он просто обрабатывает какие-то данные с помощью каких-то функций в нескольких потоках.
 * Он ничего не знает о задаче, которую решает.
 * Здесь не должно быть кода, завязанного на конкретную задачу - определение длины префикса.
 * 
 * Это наш самописный аналог hadoop mapreduce.
 * Он на самом деле не работает с по-настоящему большими данными, потому что выполняется на одной машине.
 * Но мы делаем вид, что данных много, и представляем, что наши потоки - это процессы на разных узлах.
 * 
 * Ни один из потоков не должен полностью загружать свои данные в память или пробегаться по всем данным.
 * Каждый из потоков обрабатывает только свой блок.
 * 
 * На самом деле даже один блок данных не должен полностью грузиться в оперативку, а должен обрабатываться построчно.
 * Но в домашней работе можем этим пренебречь и загрузить один блок в память одним потоком.
 */
#include <vector>
#include <filesystem>
#include <fstream> 
#include <functional>
#include <thread>
#include <string>

#include "ExternalMergeSort.h"
#include <numeric>
#include <sstream>
#include <algorithm>

class MapReduce
{
public:

    MapReduce(size_t _mappers_count, size_t _reducers_count)
        : mappers_count(_mappers_count), reducers_count(_reducers_count)
    {

    }

    void set_mapper(std::function<std::pair<std::string, std::string>(std::string)> _mapper)
    {
        mapper = _mapper;
    }

    void set_reducer(std::function<bool(std::string, std::pair<std::string, std::string>)> _reducer)
    {
        reducer = _reducer;
    }

    void run(const std::filesystem::path& input, const std::filesystem::path& output)
    {
        auto blocks = split_input_file(input, mappers_count);

        // Создаём mappers_count потоков
        // В каждом потоке читаем свой блок данных
        // Применяем к строкам данных функцию mapper
        // Сортируем результат каждого потока
        // Результат сохраняется в файловую систему (представляем, что это большие данные)
        // Каждый поток сохраняет результат в свой файл (представляем, что потоки выполняются на разных узлах)

        std::vector<std::thread> mapper_thread_pool;
        for(size_t i=0; i < mappers_count; ++i)
        {
            auto mapper_thread = std::thread(&MapReduce::mapper_do_work, this, std::ref(input), std::ref(blocks[i]));
            mapper_thread_pool.emplace_back(std::move(mapper_thread));
        }
        join_threads(mapper_thread_pool);

        //Выполняем многопутевое слияние получившихся отсортированных файлов на этапе Map.
        //На выходе получаем один отсортированный файл. Разбиваем его на reduce_count файлов для фазы reduce. 
        //В каком-то смысле это узкое место, т.к. такой файл нужно где-то хранить. 
        //С другой стороны, в дальнейшей обработке он не участвует, т.к. разбивается на более мелкие файлы.
        //К тому же его длина равна длине исходного файла, который тоже где-то нужно хранить, поэтому такое допущение считаю оправданным (храним условно там же)

        auto merge_sorted_fname = "merge_sorted";
        mergeFiles<std::string>(merge_sorted_fname, "mapped", mappers_count);

        //Cоздаем reduce_count новых файлов
        //Читаем результат фазы map и перекладываем в reducers_count (вход фазы reduce)
        // Перекладываем так, чтобы:
        //     * данные были отсортированы
        //     * одинаковые ключи оказывались в одном файле, чтобы одинаковые ключи попали на один редьюсер
        //     * файлы примерно одинакового размера, чтобы редьюсеры были загружены примерно равномерно
        // Для упрощения задачи делаем это в один поток
        // Но все данные в память одновременно не загружаем, читаем построчно и пишем

        size_t total_lines_count = 0;
        for (const auto& block : blocks)
            total_lines_count += block.lines_count;

        auto reduced_file_names = create_reduced_files(merge_sorted_fname, total_lines_count);

        // Создаём reducers_count потоков
        // В каждом потоке читаем свой файл (выход предыдущей фазы)
        // Применяем к строкам функцию reducer
        // Результат сохраняется в файловую систему 
        // (во многих задачах выход редьюсера - большие данные, хотя в нашей задаче можно написать функцию reduce так, чтобы выход не был большим)

        std::vector<std::thread> reducer_thread_pool;
        for (size_t i = 0; i < reducers_count; ++i)
        {
            auto reducer_thread = std::thread(&MapReduce::reducer_do_work, this, std::ref(reduced_file_names[i]));
            reducer_thread_pool.emplace_back(std::move(reducer_thread));
        }
        join_threads(reducer_thread_pool);


        //Пишем результаты в файл output
        std::ofstream results(output);
        int cnt = 0;
        for (const auto& file : reduced_file_names)
        {
            auto m_file = std::ifstream(file + "_output");
            std::string line;
            std::getline(m_file, line);
            results << line;
            if (cnt!= reduced_file_names.size()-1)
                results << std::endl;
            ++cnt;

            m_file.close();
        }
        results.close();
    }

private:
    struct Block
    {
        size_t from;
        size_t to;

        size_t num;
        size_t lines_count;
    };
    std::vector<Block> split_input_file(const std::filesystem::path& path, size_t blocks_count) const
    {
        /**
         * Эта функция не читает весь файл.
         *
         * Определяем размер файла в байтах.
         * Делим размер на количество блоков - получаем границы блоков.
         * Читаем данные только вблизи границ.
         * Выравниваем границы блоков по границам строк.
         */
        std::vector<Block> blocks;
        auto fsize = std::filesystem::file_size(path);
        auto block_bounds = fsize / blocks_count;
        auto elapsed_bytes = fsize - block_bounds * blocks_count;
        std::ifstream file(path);

        uintmax_t pos1 = 0;
        auto pos2 = block_bounds + elapsed_bytes;
        for (size_t i = 0; i < blocks_count; ++i)
        {
            Block block;
            file.seekg(pos2);
            char ch = ' ';
            int elapsed = 0;
            while (file.get(ch) && ch != '\n')
            {
                --pos2;
                ++elapsed;
                file.seekg(pos2);
            }
            block.from = pos1;
            block.to = pos2;
            pos1 = pos2 + 1;
            pos2 = pos1 + block_bounds + elapsed;

            block.num = i;
            blocks.push_back(block);
        }
        file.close();
        return blocks;
    }

    void mapper_do_work(const std::filesystem::path& input, Block& block) const
    {   
        //Read input file
        std::vector<std::string> data;
        read_input_file(input, block, data);
        //Map
        std::vector<std::pair<std::string, std::string>> map_output;
        std::transform(data.begin(), data.end(), std::back_inserter(map_output), mapper);
        //Sort
        std::sort(map_output.begin(), map_output.end());
        //Write to output mapped file
        write_to_mapped_file(block, map_output);
    }

    void reducer_do_work(const std::string& fname) const
    {
        //Read reduced file
        std::vector<std::pair<std::string, std::string>> data;
        read_reduced_file(fname, data);
        //Reduce
        std::vector<bool> reduce_output;

        std::string previous_data;
        for (const auto& el : data)
        {
            reduce_output.emplace_back(reducer(previous_data, el));
            previous_data = el.first;
        }
        //Write to output file
        write_to_output_file(fname, reduce_output);
    }

    void join_threads(std::vector<std::thread>& thread_pool) const
    {
        for (size_t i = 0; i < thread_pool.size(); ++i)
        {
            if (thread_pool[i].joinable())
                thread_pool[i].join();
        }
    }

    void read_input_file(const std::filesystem::path& input, Block& block, std::vector<std::string>& data) const
    {
        std::ifstream input_file(input);

        char ch;
        std::string str;
        input_file.seekg(block.from);

        while (input_file.get(ch) && input_file.tellg()<=block.to)
        {
            if (ch != '\n')
                str += ch;
            else
            {
                if (!str.empty())
                    data.push_back(str);

                str.clear();
            }
        }
        if (!str.empty())
            data.push_back(str);

        input_file.close();

        block.lines_count = data.size();
    }

    void write_to_mapped_file(Block& block,const std::vector<std::pair<std::string, std::string>>& map_output) const
    {
        std::ofstream mapped_file("mapped_" + std::to_string(block.num));

        size_t counter = 0;
        auto map_size = map_output.size();
        for (const auto& el : map_output)
        {
            mapped_file << el.first << " " << el.second;
            if (counter != (map_size - 1))      //если это последний элемент
                mapped_file << std::endl;       //перенос строки не нужен
            ++counter;
        }
        mapped_file.close();
    }

    std::vector<std::string> create_reduced_files(const std::string& merge_sorted_fname, size_t total_lines_count) const
    {
        std::vector<std::ofstream> reduced_files;
        reduced_files.reserve(reducers_count);

        std::vector<std::string> reduced_file_names;
        reduced_file_names.reserve(reducers_count);
        for (size_t i = 0; i < reducers_count; ++i)
        {
            auto f_name = "reduce_" + std::to_string(i);
            reduced_files.emplace_back(f_name);
            reduced_file_names.emplace_back(f_name);
        }

        auto file_bounds = total_lines_count / reducers_count;
        auto elapsed_lines = total_lines_count - file_bounds * reducers_count;
        auto lines_in_file = file_bounds + elapsed_lines;

        std::ifstream merge_sorted_file(merge_sorted_fname);
        size_t lines_added = 0;
        auto reduced_file_pos = 0;

        std::string new_line, last_line;

        while (std::getline(merge_sorted_file, new_line))
        {
            if (last_line != new_line && lines_added == lines_in_file - 1 && reduced_file_pos != reducers_count - 1)  //осталась последняя строка для записи в старый файл, но в ней новые данные
            {
                //пишем в новый файл, если он есть
                ++reduced_file_pos;

                reduced_files[reduced_file_pos] << new_line;
                lines_added = 1;
                lines_in_file = file_bounds + 1;
            }
            else
            {
                if (last_line == new_line && lines_added == 0)   //хотим писать в новый файл, но строка совпадает с последней строкой предыдущего
                {
                    //пишем в старый файл
                    auto last_file_pos = reduced_file_pos - 1;
                    reduced_files[last_file_pos] << std::endl << new_line;
                    --lines_in_file;
                }
                else
                {
                    if (lines_added != 0)
                        reduced_files[reduced_file_pos] << std::endl;
                    reduced_files[reduced_file_pos] << new_line;

                    ++lines_added;
                    if (lines_added == lines_in_file)
                    {
                        ++reduced_file_pos;

                        lines_added = 0;
                        lines_in_file = file_bounds;
                    }
                }
            }
            last_line = new_line;
        }

        for (auto& file : reduced_files)
            file.close();

        return reduced_file_names;
    }

    void read_reduced_file(const std::string& reduced_fname, std::vector<std::pair<std::string, std::string>>& data) const
    {
        std::string line;

        std::ifstream reduced_file(reduced_fname);
        while (std::getline(reduced_file, line))
        {
            std::stringstream ss(line);
            std::string first, second;
            std::getline(ss, first, ' ');
            std::getline(ss, second, ' ');
            data.emplace_back(std::make_pair(first,second));
        }
        reduced_file.close();
    }

    void write_to_output_file(const std::string& fname, std::vector<bool> reduce_output) const
    {
        std::ofstream reduced_file(fname+"_output");

        /*
        Запись всех результатов в файл - скорее всего это избыточно.
        size_t counter = 0;
        auto reduce_size = reduce_output.size();
        for (const auto& el : reduce_output)
        {
            reduced_file << el;
            if (counter != (reduce_size - 1))     //если это последний элемент
                reduced_file << std::endl;       //перенос строки не нужен
            ++counter;
        }*/

        /*Достаточно записать, было ли хоть раз получено false в результате работы reducerа с текущим файлом (вектор reduce_output)*/
        if (std::find(reduce_output.begin(),reduce_output.end(),false)==reduce_output.end())
            reduced_file << "1";
        else
            reduced_file << "0";

        reduced_file.close();
    }

    size_t mappers_count;
    size_t reducers_count;

    std::function<std::pair<std::string, std::string>(std::string)> mapper;
    std::function<bool(std::string, std::pair<std::string, std::string>)> reducer;
};