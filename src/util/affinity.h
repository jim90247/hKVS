#pragma once

#include <thread>
#include <pthread.h>

void SetAffinity(std::thread& t, unsigned int core);
