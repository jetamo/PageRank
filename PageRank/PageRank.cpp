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


int n = 0;
float d = 0.85;
float threshold = 1.0e-10;

std::vector<std::unordered_map<int, std::vector<int>>> graph_chunks(NUM_THREADS);

std::unordered_map<int, std::vector<int>> graph;
std::unordered_map<int, int> outbound_edges;
std::unordered_map<int, float> rank;

int* nodes = NULL;

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
    std::cout << " " << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << " milliseconds\n" << std::endl;
}

bool is_number(const std::string& s)
{
    std::string::const_iterator it = s.begin();
    while (it != s.end() && std::isdigit(*it)) ++it;
    return !s.empty() && it == s.end();
}

bool load_data(LPCWSTR filepath) {
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
                graph_chunks[i][stoi(to)].push_back(stoi(from));
                if (graph_chunks[i].find(stoi(from)) == graph_chunks[i].end()) {
                    graph_chunks[i][stoi(from)];
                }
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
        for (auto x : graph_chunks[i])
        {
            int key = x.first;
            int m = x.second.size();
            //if (std::count(nodes.begin(), nodes.end(), key) == 0)
            //    nodes.push_back(key);
            //if (graph.find(key) == graph.end())
            //    graph[key];
            for (int j = 0; j < m; j++) {
                int val = x.second[j];
                if (outbound_edges.find(key) == outbound_edges.end()) {
                    outbound_edges[key] = 0;
                }
                if (outbound_edges.find(val) == outbound_edges.end()) {
                    outbound_edges[val] = 1;
                }
                else {
                    outbound_edges[val]++;
                }
                //if (std::count(graph[key].begin(), graph[key].end(), val) == 0)
                //    graph[key].push_back(val);                   
            }
        }
    }
    n = outbound_edges.size();
    nodes = new int[n];
}


void set_initial_rank() {
    int it = 0;
    for (auto const& x : outbound_edges)
    {
        int key = x.first;
        rank[key] = 1.f / n;
        nodes[it] = key;
        it++;
    }
}

void calculate_page_rank() {
    bool done = false;
    while (!done) {
        float b = (1 - d) / n;
        done = true;
#pragma omp parallel for schedule(dynamic)
        for (int i = 0; i < n; i++) {
            int node = nodes[i];
            float sum = 0.f;

#pragma omp simd reduction(+:sum)
            for (int j = 0; j < NUM_THREADS; j++) {
                if (graph_chunks[j].find(node) != graph_chunks[j].end()) {
                    for (int k = 0; k < graph_chunks[j][node].size(); k++) {
                        sum += rank[graph_chunks[j][node][k]] / outbound_edges[graph_chunks[j][node][k]];
                    }
                }
            }
            float val = b + d * sum;
            float original_rank = rank[node];
            rank[node] = val;
            if (std::abs(original_rank - val) > threshold)
                done = false;
        }
    }
}

void print_ranks(int num) {
    int it = 0;
    for (auto x : rank) {
        it++;
        if (it >= num)
            break;
        std::cout << "Node " << x.first << "; rank " << x.second << std::endl;
    }
}


int main()
{
    start_timer();
    LPCWSTR fn = L"BerkStan.txt";
    load_data(fn);
    combine_graph_chunks();
    set_initial_rank();
    end_timer();
    std::cout << "Data loaded in: ";
    print_elapsed_time();
    start_timer();
    calculate_page_rank();
    print_ranks(10);
    end_timer();
    std::cout << "Rank calculated in: ";
    print_elapsed_time();
    return 0;
}
