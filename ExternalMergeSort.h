#pragma once
/*����� ������:
https://www.techiedelight.com/ru/external-merge-sort/
������� ��������� ���������:
- �������� �������� �������� ������ �������� ���������� (��� MSVS �� ���������������);
- ������ � ���������� (������, ����������) ������ ����� �����;
- ������������� fstream ������ ����������� FILE*;
- ���������� ���������� ������ (�� ������� �� ���������� ���� � ������ ������������� ���������).
- ������� createInitialRuns � ���������� MapReduceFramework� ��������� �� �����, �.�. � �������� ��������� mappper_count ������� �� ���� Map
*/

#include <iostream>
#include <algorithm>
#include <queue>
#include <limits>
#include <vector>

template <typename T>
struct MinHeapNode
{
    // ������� ��� ����������
    T element;

    // ������ �������, �� �������� ������� �������
    int i=0;
};

// ������ ���������, ������� ����� �������������� ��� �������������� ����
template <typename T>
struct comp
{
    bool operator()(const MinHeapNode<T>& lhs, const MinHeapNode<T>& rhs) const 
    {
        return lhs.element > rhs.element;
    }
};

// ���������� `k` ��������������� ������. ��������������, ��� ����� ������ <input_filename_prefix>_(1, 2, � `k`)
template <typename T>
void mergeFiles(const std::string& output_file, const std::string& input_filename_prefix, size_t k)
{
    std::vector<std::ifstream> in;
    in.reserve(k);
    for (int i = 0; i < k; i++)
    {
        // ��������� �������� ����� � ������ ������
        in.emplace_back(input_filename_prefix + "_" + std::to_string(i));
    }

    //�������� ����
    std::ofstream out(output_file);

    // ������� ����-���� � `k` ����� ����. ������ ���� ���� ����� ������
    // ������� �������� ����� ������
    std::vector<MinHeapNode<T>> harr;
    harr.reserve(k);
    std::priority_queue<MinHeapNode<T>, std::vector<MinHeapNode<T>>, comp<T>> pq;

    int i;
    for (i = 0; i < k; i++)
    {
        // ���������, ���� ��� ������� ��������� ����� �
        // ������ `i` ����� ����������� ������� ������
        MinHeapNode<T> node;
        harr.emplace_back(std::move(node));

        T data;
        if (!std::getline(in[i], data))
        {
            break;
        }
        harr[i].element = data;

        // ������ �������� ����� ������
        harr[i].i = i;
        pq.push(harr[i]);
    }

    auto default_element = std::numeric_limits<T>::max();
    do
    {
        int count = 0;
        // ���� �� ������ �������� ����������� ������� �� ����������� ���� � ������
        // ��� �� ��������� ���������. ��������� �� ��� ���, ���� ��� ����������� ������� ����� �� ��������� EOF.
        while (count != i)
        {
            // �������� ����������� ������� � ��������� ��� � �������� �����
            MinHeapNode<T> root = pq.top();
            pq.pop();
            out << root.element;

            // ������� ��������� �������, ������� ������ �������� ������� ������ ����.
            // ��������� ������� ����������� ���� �� �������� �����, ��� � ������� ����������� �������.
            if (!std::getline(in[root.i], root.element))
            {
                root.element = default_element;
                count++;
            }
            if (count != i)
                out << std::endl;

            // �������� ������ ��������� ��������� �������� �����
            if (root.element != default_element)
            {
                pq.push(root);
            }
        }
    }
    while (!pq.empty());

    // ��������� ������� � �������� �����
    for (int i = 0; i < k; i++) 
    {
        in[i].close();
    }

    out.close();
}

// ��������� ����������� �������� ����������, ������ ��������� ������� � ��������� ��
// ���������� ����� �������� ������
template <typename T>
void createInitialRuns(const std::string& input_file, int run_size, int num_ways)
{
    // ��� �������� �������� �����
    std::ifstream in(input_file);

    // ����� ������� ������
    std::vector<std::ofstream> out;
    out.reserve(num_ways);
    for (int i = 0; i < num_ways; i++)
    {
        // ��������� �������� ����� � ������ ������
        out.emplace_back(std::to_string(i));
    }

    // �������� ������������ ������, ���������� ������� ��� ���������� ��������
    // ������ `run_size`
    std::vector<T> arr;
    arr.reserve(run_size);

    bool more_input = true;
    int next_output_file = 0;

    int i;
    while (more_input)
    {
        // �������� �������� `run_size` � `arr` �� �������� �����
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

        // ��������� ������ �� run_size ���������
        std::sort(arr.begin(), arr.end());

        // ���������� ������ � ��������������� ��������� �������� ����
        for (int j = 0; j < i; j++) 
        {
            out[next_output_file] << arr[j];
            if (j!=i-1)
                out[next_output_file] << std::endl;
        }
        arr.clear();
        next_output_file++;
    }

    // ��������� ������� � �������� �����
    for (int i = 0; i < num_ways; i++) 
    {
        out[i].close();
    }

    in.close();
}