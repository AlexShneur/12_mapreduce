#pragma once
/*Взято отсюда:
https://www.techiedelight.com/ru/external-merge-sort/
Внесены следующие изменения:
- создание векторов объектов вместо массивов указателей (под MSVS не компилировалось);
- работа с темлейтами (чтение, сортировка) вместо целых чисел;
- использование fstream вместо устаревшего FILE*;
- исправлены логические ошибки (из коробки не заработало даже с учётом вышеуказанных изменений).
- функция createInitialRuns в реализации MapReduceFrameworkа оказалась не нужна, т.к. её действия выполняют mappper_count потоков на фазе Map
*/

#include <iostream>
#include <algorithm>
#include <queue>
#include <limits>
#include <vector>

template <typename T>
struct MinHeapNode
{
    // элемент для сохранения
    T element;

    // индекс массива, из которого берется элемент
    int i=0;
};

// Объект сравнения, который будет использоваться для упорядочивания кучи
template <typename T>
struct comp
{
    bool operator()(const MinHeapNode<T>& lhs, const MinHeapNode<T>& rhs) const 
    {
        return lhs.element > rhs.element;
    }
};

// Объединяет `k` отсортированных файлов. Предполагается, что имена файлов <input_filename_prefix>_(1, 2, … `k`)
template <typename T>
void mergeFiles(const std::string& output_file, const std::string& input_filename_prefix, size_t k)
{
    std::vector<std::ifstream> in;
    in.reserve(k);
    for (int i = 0; i < k; i++)
    {
        // открываем выходные файлы в режиме чтения
        in.emplace_back(input_filename_prefix + "_" + std::to_string(i));
    }

    //выходной файл
    std::ofstream out(output_file);

    // Создаем мини-кучу с `k` узлов кучи. Каждый узел кучи имеет первый
    // элемент рабочего файла вывода
    std::vector<MinHeapNode<T>> harr;
    harr.reserve(k);
    std::priority_queue<MinHeapNode<T>, std::vector<MinHeapNode<T>>, comp<T>> pq;

    int i;
    for (i = 0; i < k; i++)
    {
        // прерываем, если нет пустого выходного файла и
        // индекс `i` будет количеством входных файлов
        MinHeapNode<T> node;
        harr.emplace_back(std::move(node));

        T data;
        if (!std::getline(in[i], data))
        {
            break;
        }
        harr[i].element = data;

        // индекс рабочего файла вывода
        harr[i].i = i;
        pq.push(harr[i]);
    }

    auto default_element = std::numeric_limits<T>::max();
    do
    {
        int count = 0;
        // Один за другим получаем минимальный элемент из минимальной кучи и меняем
        // его со следующим элементом. Выполнять до тех пор, пока все заполненные входные файлы не достигнут EOF.
        while (count != i)
        {
            // Получить минимальный элемент и сохранить его в выходном файле
            MinHeapNode<T> root = pq.top();
            pq.pop();
            out << root.element;

            // Находим следующий элемент, который должен заменить текущий корень кучи.
            // Следующий элемент принадлежит тому же входному файлу, что и текущий минимальный элемент.
            if (!std::getline(in[root.i], root.element))
            {
                root.element = default_element;
                count++;
            }
            if (count != i)
                out << std::endl;

            // Заменяем корень следующим элементом входного файла
            if (root.element != default_element)
            {
                pq.push(root);
            }
        }
    }
    while (!pq.empty());

    // закрываем входной и выходной файлы
    for (int i = 0; i < k; i++) 
    {
        in[i].close();
    }

    out.close();
}

// Используя стандартный алгоритм сортировки, создаём начальные прогоны и разделяем их
// равномерно среди выходных файлов
template <typename T>
void createInitialRuns(const std::string& input_file, int run_size, int num_ways)
{
    // Для большого входного файла
    std::ifstream in(input_file);

    // вывод рабочих файлов
    std::vector<std::ofstream> out;
    out.reserve(num_ways);
    for (int i = 0; i < num_ways; i++)
    {
        // открываем выходные файлы в режиме записи
        out.emplace_back(std::to_string(i));
    }

    // выделяем динамический массив, достаточно большой для размещения прогонов
    // размер `run_size`
    std::vector<T> arr;
    arr.reserve(run_size);

    bool more_input = true;
    int next_output_file = 0;

    int i;
    while (more_input)
    {
        // записать элементы `run_size` в `arr` из входного файла
        for (i = 0; i < run_size; i++)
        {
            T data;
            if (!std::getline(in, data))
            {
                more_input = false;
                break;
            }
            arr.emplace_back(data);
        }

        // сортируем прогон из run_size элементов
        std::sort(arr.begin(), arr.end());

        // записываем записи в соответствующий временный выходной файл
        for (int j = 0; j < i; j++) 
        {
            out[next_output_file] << arr[j];
            if (j!=i-1)
                out[next_output_file] << std::endl;
        }
        arr.clear();
        next_output_file++;
    }

    // закрываем входной и выходной файлы
    for (int i = 0; i < num_ways; i++) 
    {
        out[i].close();
    }

    in.close();
}