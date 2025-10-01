# Makefile

# C와 C++ 공용 플래그 (주로 헤더 경로)
CPPFLAGS := -I ../libhrd/ -I ../mica/

# C 전용 플래그에 공용 플래그를 추가
CFLAGS   := -O3 -Wall -Werror -Wno-unused-result $(CPPFLAGS)

# C++ 전용 플래그를 새로 만들고 공용 플래그를 추가
CXXFLAGS := -O3 -Wall -Werror -Wno-unused-result $(CPPFLAGS)

# 링커 및 링커 플래그
LD       := g++
LDFLAGS  := -libverbs -lrt -lpthread -lmemcached -lnuma -lboost_system -lboost_coroutine -lboost_context

# --- 이 아래는 그대로 둡니다 ---
APPS     := main

all: ${APPS}

main: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o ../mica/mica.o ../mica/city.o main.o client.o master.o worker.o
	${LD} -o $@ $^ ${LDFLAGS}

PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ../mica/*.o ${APPS}
