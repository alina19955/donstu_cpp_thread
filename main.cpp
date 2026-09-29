#include <iostream>
#include <fstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string>


class Logger {
private:
    std::ofstream log_file;
    std::mutex log_mtx;

public:
    Logger(const std::string& filename) {
        log_file.open(filename, std::ios::out);
    }

    ~Logger() {
        if (log_file.is_open()) {
            log_file.close();
        }
    }

    void writeLine(const std::string& text) {
        std::lock_guard<std::mutex> lock(log_mtx);
        if (log_file.is_open()) {
            log_file << text << std::endl;
        }
    }
};


std::mutex m;
std::condition_variable cv;

int buffer = 0;       
bool ready = false;     
bool done = false;     


void producer() {
    for (int i = 1; i <= 10; ++i) {
        {
            std::unique_lock<std::mutex> lock(m);
    
            cv.wait(lock, [] { return !ready; });

            buffer = i;   // Кладем значение в буфер
            ready = true; // Сигнализируем, что данные готовы
        }
        cv.notify_one();  // Будим потребителя
    }


    {
        std::unique_lock<std::mutex> lock(m);

        cv.wait(lock, [] { return !ready; });
        done = true; 
    }
    cv.notify_one(); 
}


void consumer(Logger& logger) {
    while (true) {
        int item = 0;
        {
            std::unique_lock<std::mutex> lock(m);
      
            cv.wait(lock, [] { return ready || done; });

      
            if (!ready && done) {
                break;
            }

            item = buffer; // Забираем значение из буфера
            ready = false; // Помечаем буфер как пустой
        }
        cv.notify_one(); 

        // Запись в файл через logger
        logger.writeLine("[Consumer] Забрал значение: " + std::to_string(item));
    }
}

int main() {
    Logger logger("output.log");

    std::thread prod(producer);
    std::thread cons(consumer, std::ref(logger));

    prod.join();
    cons.join();

    std::cout << "Передача завершена. Проверьте output.log" << std::endl;

    return 0;
}
