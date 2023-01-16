#include <iostream>
#include <unordered_map>
#include <vector>
#include <string>
#include <fstream>
#include <chrono>
#include <future>
#include <windows.h>

std::string filename = "BerkStan.txt";

const int NUM_THREADS = 100;

std::vector<std::unordered_map<int, std::vector<int>>> graph_chunks(NUM_THREADS);

std::unordered_map<int, std::vector<int>> graph;

std::chrono::high_resolution_clock::time_point t1;
std::chrono::high_resolution_clock::time_point t2;


void start_timer() {
    std::cout << "Timer started" << std::endl;
    t1 = std::chrono::high_resolution_clock::now();
}

void end_timer() {
    t2 = std::chrono::high_resolution_clock::now();
    std::cout << "Timer ended" << std::endl;
}
void print_elapsed_time() {
    std::cout << "Time elapsed: " << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << " milliseconds\n" << std::endl;
}

bool is_number(const std::string& s)
{
    std::string::const_iterator it = s.begin();
    while (it != s.end() && std::isdigit(*it)) ++it;
    return !s.empty() && it == s.end();
}

bool ParallelReadTxtFile(LPCWSTR filepath) {
    HANDLE hFile = CreateFile(filepath, GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
        std::cout << "Error: Could not open file" << std::endl;
        return false;
    }

    HANDLE hMap = CreateFileMapping(hFile, NULL, PAGE_READONLY, 0, 0, NULL);
    if (hMap == NULL) {
        std::cout << "Error: Could not create file mapping" << std::endl;
        CloseHandle(hFile);
        return false;
    }

    LPVOID lpMap = MapViewOfFile(hMap, FILE_MAP_READ, 0, 0, 0);
    if (lpMap == NULL) {
        std::cout << "Error: Could not map view of file" << std::endl;
        CloseHandle(hMap);
        CloseHandle(hFile);
        return false;
    }

    char* map = static_cast<char*>(lpMap);
    DWORD fileSize = GetFileSize(hFile, NULL);
    DWORD chunkSize = fileSize / NUM_THREADS;

    std::thread threads[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
        DWORD start = i * chunkSize;
        DWORD end = (i + 1) * chunkSize;
        if (i == NUM_THREADS - 1) {
            end = fileSize;
        }
        threads[i] = std::thread([=](DWORD start, DWORD end) {
            char* current = map + start;
            char* endPointer = map + end;
            while (current < endPointer) {
                std::string line;
                while (*current != '\n' && current < endPointer) {
                    line += *current;
                    current++;
                }
                current++;
                std::string delimiter = "\t";
                std::string delimiter2 = "\r";
                std::string from = line.substr(0, line.find(delimiter));
                std::string to = line.substr(line.find(delimiter) + 1, line.find(delimiter2) - line.find(delimiter) - 1);
                bool num = is_number(from) && is_number(to);
                if (!num)
                    continue;
                graph_chunks[i][stoi(from)].push_back(stoi(to));
                //mtx.lock();
                //graph[stoi(from)].push_back(stoi(to));
                //mtx.unlock();
            }
            }, start, end);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i].join();
    }

    UnmapViewOfFile(lpMap);
    CloseHandle(hMap);
    CloseHandle(hFile);

    return true;
}

void combine_graph_chunks() {
    for (int i = 0; i < graph_chunks.size(); i++) {
        for (auto const& x : graph_chunks[i])
        {
            int key = x.first;
            for (int j = 0; j < x.second.size(); j++) {
                if (std::find(graph[key].begin(), graph[key].end(), x.second[j]) != graph[key].end())
                    graph[key].push_back(x.second[j]);
            }
        }
    }
}

void load_file_part(int start, int end, std::ifstream &file, std::vector<std::string> &lines)
{
    //std::ifstream file(filename);
    file.seekg(start);
    std::string line;
    //std::vector<std::string> t;
    while (file.tellg() < end && std::getline(file, line)) {
        lines.push_back(line);
        //std::cout << start << std::endl;
    }
}

void tmp() {
    int number_of_threads = 10;
    std::vector<std::future<void>> threads;
    std::vector<std::vector<std::string>> lines(number_of_threads);
    std::ifstream file(filename);
    file.seekg(0, std::ios::end);
    int length = file.tellg();
    

    int part_size = length / number_of_threads; // divide the file into 4 parts

    for (int i = 0; i < number_of_threads; i++) {
        int from = part_size * i;
        int to = part_size * (i + 1);
        if (i == number_of_threads - 1)
            to = length;
        threads.push_back(std::async(std::launch::async, load_file_part, from, to, std::ref(file), std::ref(lines[i])));
    }
    for (int i = 0; i < number_of_threads; i++) {
        threads[i].wait();
    }
    file.close();
    int sum = 0;
    for (int i = 0; i < lines.size(); i++) {
        sum += lines[i].size();
    }
    std::cout << sum << std::endl;
}

void load_data() {
    std::fstream newfile;
    newfile.open("BerkStan.txt", std::ios::in); 
    std::ifstream file("BerkStan.txt");
    if (!file.is_open()) {
        std::cout << "Failed to open file: " << std::endl;
    }

    int from, to;
    while (file >> from >> to) {
        graph[from].push_back(to);
        //aa.push_back(std::to_string(from));
    }

}

int main()
{
    start_timer();
    LPCWSTR fn = L"BerkStan.txt";
    ParallelReadTxtFile(fn);
    //tmp();
    //load_data();
    combine_graph_chunks();
    end_timer();
    print_elapsed_time();
    //int sum = 0;
    //for (int i = 0; i < graph_chunks.size(); i++) {
    //    sum += graph_chunks[i].size();
    //}
    std::cout << graph.size() << std::endl;
    return 0;
}
