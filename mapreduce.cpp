#include <iostream>
#include <fstream>
#include <thread>
#include <mutex>
#include <vector>
#include <queue>
#include <condition_variable>
#include <functional>
#include <string>
#include <map>
#include <cstdio>
#include <cstring>
#include <vector>
#include <chrono>
#include <unistd.h>
#include <set>

using namespace std;
#define MAX_WORK 1000	//한 파일에 저장할 수

class ThreadPool {
private:
    vector<thread> workers; //쓰레드 벡터
    queue<function<void()>> jobs; //작업 큐, 함수 포인터로 저장. 반환값이 void
    mutex queue_mutex;	//뮤텍스
    condition_variable condition;	//조건 변수, 생산자-소비자 패턴으로 쓰레드 동기화시 필요
    bool stop;	//종료 변수, 쓰레드 풀 소멸될 때 true, 쓰레드들이 종료 되야함 을 알림
    size_t counter;	//대기중인 작업 개수, 0일때 모든 작업이 종료됨을 알수 있음
public:
    ThreadPool(size_t threads) : stop(false), counter(0) { //생성자, threads=스레드 개수, stop=false, counter=0으로 초기화
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] { //벡터에 람다식을 추가, this를 내부에서 사용가능하게 함
                while (true) {
                    function<void()> task;	//작업 함수
                    {
                        unique_lock<mutex> lock(this->queue_mutex);	//뮤텍스를 잠금
                        this->condition.wait(lock, [this] { return this->stop || !this->jobs.empty(); });	//작업이 비어있게 되거나 종료될 때까지 대기
                        if (this->stop && this->jobs.empty()) break;	//작업이 없고 종료해야 할 시 루프를 종료하여 작업 읽기를 끝냄
                        task = move(this->jobs.front());	//작업을 큐에서 가져옴
                        this->jobs.pop();	//꺼낸 작업 삭제
                    }

                    task();	//작업 실행

                    {
                        unique_lock<mutex> lock(this->queue_mutex);	//작업을 마친 후 뮤텍스를 잠그고 --counter
                        --counter;
                    }
                }
            });
        }
    }

    template <class F, class... Args>	//가변길이 템플릿. 임의의 개수를 인자로 받을 수 있음
    void enqueue(F&& f, Args&&... args) {
        auto result = bind(forward<F>(f), forward<Args>(args)...); //함수에 인자를 바인딩하여 새로운 함수 객체를 생성
        {
            unique_lock<mutex> lock(queue_mutex);	//뮤텍스 잠금
            if (stop) throw runtime_error("enqueue on stopped ThreadPool");	//stop==true일시 스레드풀은 종료된 상태이기 때문에 에러 처리
            jobs.emplace(result); //작업큐에 함수 삽입
            ++counter; 	//작업 카운트 ++
        }
        condition.notify_one();	//스레드 하나를 깨워 작업을 수행하도록 함
    }

    bool is_empty() { 	//작업 큐가 비어있는지 확인하는 함수
        unique_lock<mutex> lock(queue_mutex);
        return counter == 0 && jobs.empty();
    }

    ~ThreadPool() {		//소멸자
        {
            unique_lock<mutex> lock(queue_mutex);
            stop = true;	//stop변수 true설정
        }
        condition.notify_all();	//모든 스레들에게 알림
        for (thread& worker : workers)	//스레들이 종료되기를 기다림
            worker.join();
    }
};

map<string, int> datas;
vector<string> map_names;
vector<int> map_offset;
mutex datas_mutex;  // datas에 대한 동시 접근을 막기 위한 뮤텍스 추가

int mapper(char folder_address[], char input_address[]);
void onReducer(char folder_address[], string, string);
void externalSort(char folder_address[], char output_address[]);
void saveOutput(map<string, int>& map, char output_address[]);
void saveOutput(string word, int wordCount, char output_address[]);

void readFileAndPrint(const std::string& filename) {
    std::ifstream file(filename);
    char a[100];
    strcpy(a, filename.c_str());
    if (file.is_open()) {
        std::string line;
        printf("\nContents of %s\n", a);

        while (std::getline(file, line)) {
            std::cout << "\n" << line << '\n';
        }
        file.close();
    } else {
        std::cerr << "Unable to open file: " << filename << std::endl;
    }
}

int main() {
    int thd_num;
    printf("몇개의 스레드를 사용하시겠습니까? ");
    cin >> thd_num;
    getchar();
    ThreadPool pool(thd_num);	//몇개의 스레드를 작동할 것인지 선택
    int cnt = 0;
    string str_folder_address;
    string str_input_address;
    string str_output_address;
    char folder_address[100];
    char input_address[100];
    char output_address[100];
    vector<string> map_vec;
    vector<string> tmp_map_vec;
    vector<pair<string, string>> filePairs;

    cout << "**경로는 절대경로를 입력해야 합니다.**\n";
    cout << "MapReduce를 실행할 파일이 저장된 폴더의 경로를 적어주십시오.\n";
    getline(cin, str_folder_address);
    cout << "MapReduce를 실행할 파일의 경로를 적어주십시오.\n";
    getline(cin, str_input_address);
    cout << "MapReduce의 결과가 저장될 파일의 경로를 적어주십시오.\n";
    getline(cin, str_output_address);

    strcpy(folder_address, str_folder_address.c_str());
    strcpy(input_address, str_input_address.c_str());
    strcpy(output_address, str_output_address.c_str());

    chrono::system_clock::time_point start_time = chrono::system_clock::now(); //시간 측정
    printf("\n시간측정 시작!!\n");
    printf("\n파일을 분할하는 중입니다...\n");
    cnt = mapper(folder_address, input_address);
    printf("\n파일을 %d개로 분할\n", cnt + 1);
    char tmp[20];
    char tmp_2[20];

    printf("\nReduce 작업 시작 !!\n");
    for (int i = 0; i < cnt + 1; i++) {
        sprintf(tmp, "map_%d", i);
        sprintf(tmp_2, "tmp_map_%d", i);
        map_names.emplace_back(tmp_2);
        map_offset.emplace_back(0);
        filePairs.push_back(pair<string, string>(tmp, tmp_2));
    }

    for (auto& a : filePairs) {
        pool.enqueue(onReducer, folder_address, a.first, a.second);
    }

    while (!pool.is_empty()) { 
        printf("\n***Reduce 쓰레드 실행중***\n");
        sleep(1);
    }

    externalSort(folder_address, output_address);

    printf("\nReduce 작업 완료!!\n");
    printf("\n%s에 결과가 저장되었습니다.\n\n", output_address);

    chrono::system_clock::time_point end_time = chrono::system_clock::now(); //시간 측정 종료
    chrono::milliseconds mill = chrono::duration_cast<chrono::milliseconds>(end_time - start_time);
    chrono::seconds sec = chrono::duration_cast<chrono::seconds>(end_time - start_time);
    chrono::minutes min = chrono::duration_cast<chrono::minutes>(end_time - start_time);

    cout << "걸린 시간(밀리초) : " << mill.count() << "\n";
    cout << "걸린 시간(초)     : " << sec.count() << "\n";
    cout << "걸린 시간(분)     : " << min.count() << "\n";
    return 0;
}



int mapper(char folder_address[], char input_address[])
{
    FILE* fINPUT;
    FILE* MAP;
    char addr_1[300];
    char addr[300];
    char buffer1[500];
    char word[300] = "(";
    int i = 0, j, cnt = 0;

    // 입력 파일 열기
    fINPUT = fopen(input_address, "r");
    if (fINPUT == NULL) {
        printf("[MAPPER ERROR!] %s file not found\n", input_address);
        exit(0);
    }
    // 임시 파일 열기
    sprintf(addr_1, "%s/map_%d.txt", folder_address, i);
    MAP = fopen(addr_1, "w+"); // 쓰기 + 읽기 파일
    if (MAP == NULL) {
        printf("[MAPPER ERROR!!] %s files are not found\n", addr_1);
        exit(0);
    }

    // 전처리
    i = 0;
    cnt = 0;
    while (fgets(buffer1, sizeof(buffer1), fINPUT) != NULL) { //input 전체 순회
        // 대문자는 소문자로 변경
        for (j = 0; j < strlen(buffer1); j++) {
            if (buffer1[j] >= 65 && buffer1[j] <= 90) {
                buffer1[j] = buffer1[j] + 32;
            }
        }

        // 특수문자 제거
        for (j = 0; j < strlen(buffer1); j++) {
            if (buffer1[j] < 97 || buffer1[j] > 122) {
                buffer1[j] = 32;
            }
        }

        // 자르기, 나눠 저장하기
        char* ptr = strtok(buffer1, " \n");
        while (ptr != NULL) {
            cnt++;
            // 파일이 MAX_WORK를 초과하면 다음 파일로 넘어감
            if (cnt > MAX_WORK) {
                fclose(MAP);
                i++;
                sprintf(addr, "%s/map_%d.txt", folder_address, i);
                MAP = fopen(addr, "w+");
                if (MAP == NULL) {
                    printf("[MAPPER ERROR!!!] %s files are not found\n", addr);
                    exit(0);
                }
                cnt = 1;
            }

            strcpy(word, (strcat((strcat(word, ptr)), ", 1)\n")));
            fputs(word, MAP);
            strcpy(word, "(");
            ptr = strtok(NULL, " \n");
        }
    }

    fclose(fINPUT);
    fclose(MAP);
    return i;
}

void onReducer(char folder_address[], string str_map_name, string str_tmp_map_name) {
    FILE* MAP;
    FILE* TMP_MAP;
    char buffer[300];
    char addr_1[200];
    char addr_2[200];
    char addr[200];

    strcpy(addr_1, folder_address);
    strcpy(addr_2, str_map_name.c_str()); // string을 문자열로 저장
    strcpy(addr, addr_1);
    strcat(addr, "/");
    strcat(addr, addr_2);
    strcat(addr, ".txt");
    MAP = fopen(addr, "r");
    if (MAP == NULL) {
        printf("[onReduce ERROR!] %s files are not found\n", addr);
        exit(0);
    }

    // 파일 읽기
    map<string, int> tmpMap;
    while (fgets(buffer, sizeof(buffer), MAP) != NULL) {
        string sbuffer(buffer);
        string word;
        int wordCount;
        sbuffer.pop_back();
        sbuffer.pop_back();
        sbuffer = sbuffer.substr(1);
        int splitIndex = sbuffer.find(", ");
        word = sbuffer.substr(0, splitIndex);
        wordCount = stoi(sbuffer.substr(splitIndex + 2));
        tmpMap[word] += wordCount;
    }
    fclose(MAP);

    // tmp_map에 다시 저장
    strcpy(addr_2, str_tmp_map_name.c_str()); // string을 문자열로 저장
    strcpy(addr, addr_1);
    strcat(addr, "/");
    strcat(addr, addr_2);
    strcat(addr, ".txt");
    TMP_MAP = fopen(addr, "w+");
    for (auto iter = tmpMap.begin(); iter != tmpMap.end(); iter++) {
        sprintf(buffer, "(%s, %d)\n", iter->first.c_str(), iter->second);
        fputs(buffer, TMP_MAP);
    }
    fclose(TMP_MAP);
}

void externalSort(char folder_address[], char output_address[]) {
    set<int> completed; //처리가 끝난 인덱스
    int i = 0; //현재 검사중인 인덱스
    char buf[300];
    map<string, int> tmp_datas;
    while (completed.size() < map_names.size()) {
        // 모든 완료되지 않은 인덱스에 대해 작업 반복
        char addr[300];
        strcpy(addr, folder_address);
        strcat(addr, map_names[i].c_str());
        strcat(addr, ".txt");
        FILE *CUR_MAP_FILE = fopen(addr, "r");
        fseek(CUR_MAP_FILE, map_offset[i], SEEK_SET);
        if (fgets(buf, sizeof(buf), CUR_MAP_FILE) != NULL) {
            string sbuffer(buf);
            string word;
            int wordCount;
            sbuffer.pop_back();
            sbuffer.pop_back();
            sbuffer = sbuffer.substr(1);
            int splitIndex = sbuffer.find(", ");
            word = sbuffer.substr(0, splitIndex);
            wordCount = stoi(sbuffer.substr(splitIndex + 2));
            tmp_datas[word] += wordCount;
            printf("MAP: %s, KEY: %s, CNT: %d\n", map_names[i].c_str(), word.c_str(), wordCount);
            map_offset[i] = ftell(CUR_MAP_FILE);
        } else {
            // 파일 탐색이 완료되었을 경우
            completed.insert(i);
        }
        fclose(CUR_MAP_FILE);

        // 모든 파일을 한번씩 읽었을 경우 처리
        if (i + 1 == (int)map_names.size()) {
            if (datas.begin()->first == tmp_datas.begin()->first || datas.empty()) {
                // 현재 datas의 최소값과 tmp_datas의 최소값이 같거나 datas가 비어있다면 datas에 추가
                for (auto iter = tmp_datas.begin(); iter != tmp_datas.end(); iter++) {
                    datas[iter->first] += iter->second;
                }
                tmp_datas.clear();
            } else {
                // 그렇지 않다면 output에 저장 후 datas의 첫 요소 제거
                saveOutput(datas.begin()->first, datas.begin()->second, output_address);
                datas.erase(datas.begin()->first);
            }
        }
        i = (i + 1) % (int)map_names.size();
    }
    if (!datas.empty()) {
        // datas에 남은 데이터를 output에 저장
        saveOutput(datas, output_address);
    }
}

void saveOutput(map<string, int> &map, char output_address[]) {
    // datas의 모든 데이터 output에 저장
    char wbuf[300];
    FILE *fOUTPUT = fopen(output_address, "a+");
    for (auto iter = map.begin(); iter != map.end(); iter++) {
        sprintf(wbuf, "(%s, %d)\n", iter->first.c_str(), iter->second);
        fputs(wbuf, fOUTPUT);
    }
    fclose(fOUTPUT);
}

void saveOutput(string word, int wordCount, char output_address[]) {
    // datas의 모든 데이터 output에 저장
    char wbuf[300];
    FILE *fOUTPUT = fopen(output_address, "a+");
    sprintf(wbuf, "(%s, %d)\n", word.c_str(), wordCount);
    fputs(wbuf, fOUTPUT);
    fclose(fOUTPUT);
}

