#include "MapReduce.h"
#include <iostream>
#include <string>
/**
 * В этом файле находится клиентский код, который использует наш MapReduce фреймворк.
 * Этот код знает о том, какую задачу мы решаем.
 * Задача этого кода - верно написать мапер, редьюсер, запустить mapreduce задачу, обработать результат.
 * Задача - найти минимальную длину префикса, который позволяет однозначно идентифицировать строку в файле.
 * Задача не решается в одну mapreduce задачу. Нужно делать несколько запусков.
 * 
 * Выделяем первые буквы слов (в мапере), решаем для них задачу "определить, есть ли в них повторы".
 * Если не прокатило, повторяем процедуру, выделяя первые две буквы.
 * И т.д. В итоге найдём длину префикса, который однозначно определяет строку.
 */
int main(int argc, const char* argv[]) 
{
    if (argc < 4) 
    {
        std::cerr << "Usage: " << argv[0] << " <src> " << "<map threads count> " << "<reduce threads count>" << std::endl;
        return EXIT_FAILURE;
    }
    std::filesystem::path input(argv[1]);
    std::filesystem::path output("output");
    size_t mappers_count = atoi(argv[2]);
    size_t reducers_count = atoi(argv[3]);

    MapReduce mr(mappers_count, reducers_count);
    int found = 0;
    int prefix_len = 1;
    do
    {
        //  * получает строку, 
        //  * выделяет префикс, 
        //  * возвращает пары (префикс, 1).
        auto mapper = [prefix_len](const std::string& word) 
        {
            return std::pair{ word.substr(0,prefix_len), std::to_string(1) };
        };
        mr.set_mapper(mapper);

        //  * получает строку(предыдущий префикс) и пару (текущий префикс, число),
        //  * если текущий префикс совпадает с предыдущим или имеет кол-во повторов > 1, то возвращает false,
        //  * иначе возвращает true.
        auto reducer = [](std::string last_prefix, std::pair<std::string, std::string> prefix_to_repeats)
        {
            if (prefix_to_repeats.first == last_prefix || atoi(prefix_to_repeats.second.c_str())>1)
                return false;
            return true;
        };

        mr.set_reducer(reducer);

        mr.run(input, output);

        //Читаем результаты
        std::ifstream results(output);
        std::string line;
        while (std::getline(results, line))
        {
            found = std::atoi(line.c_str());
            if (!found)
            {
                ++prefix_len;
                break;
            }
        }
        results.close();
    } while (!found);

    std::cout << "Minimal prefix len = " << prefix_len;

    return EXIT_SUCCESS;
}