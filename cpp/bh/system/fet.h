#ifndef CORE_FET_H_INCLUDED
#define CORE_FET_H_INCLUDED 1

#include <map>

// #include "system/RCSystem.h"
#include "core/tools.h"

//#define FUNCTIONS_EXECUTION_TIMES

#ifdef FUNCTIONS_EXECUTION_TIMES
class FunctionsExecutionTimes;
extern FunctionsExecutionTimes* fet;
typedef std::set<PackCoordinate> LoadedDataPackCounter;
extern LoadedDataPackCounter count_distinct_dp_loads;
extern LoadedDataPackCounter count_distinct_dp_decompressions;
extern uint64 NoBytesReadByDPs;
extern uint64 SizeOfUncompressedDP;

void NotifyDataPackLoad(const PackCoordinate& coord);

void NotifyDataPackDecompression(const PackCoordinate& coord);


class FunctionsExecutionTimes
{
	struct Entry {
		_uint64		call;	// no. of calls
		_uint64		time;	// total time of calls
		Entry(_uint64 t)	{ call = 1; time = t; }
		Entry()				{}
	};

	struct SortEntry {
		SortEntry(const std::string & s, const Entry & e) : n(s), e(e) {};
		std::string n;
		Entry e;
	};

	class SortEntryLess {
	public:
		bool operator() (const SortEntry& e1, const SortEntry& e2) {
			return e1.e.time < e2.e.time;
		}
	};

public:
	FunctionsExecutionTimes()
	{
		times = new std::map<std::string, Entry>;
		flush = false;
        /******************************************
		pthread_mutexattr_t mattr;
		pthread_mutexattr_init(&mattr);
		pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE);
		pthread_mutex_init(&cs, &mattr);
        ******************************************/
		InstallFlushSignal();
	}
	~FunctionsExecutionTimes()
	{
		times->clear();
		delete times;
		//pthread_mutex_destroy(&cs);
	}

	void Add(const std::string& function_name, _uint64 time)
	{
		Lock();

		if(flush) {
			Print();
			Clear();
			flush = false;
		}

		if(times->find(function_name) != times->end())
		{
			Entry& entry = (*times)[function_name];
			entry.time += time ;
			entry.call ++;
		}
		else
		{
			//times.insert(function_name);
			Entry entry(time);
			times->insert(make_pair(function_name, entry));
		}
		UnLock();
	}

	void PrintToRcdev()
	{
		Lock();
		Print();
		UnLock();
	}

	// order flushing; it will be executed on the next Add()
	void Flush()					{ flush = true; }

	static void FlushFET(int signum);	// flush global object 'fet'
	static void InstallFlushSignal();

private:
	void Lock()		{ fet_mutex.Lock(); /*pthread_mutex_lock(&cs);*/ }
	void UnLock()	{ fet_mutex.Unlock(); /*pthread_mutex_unlock(&cs);*/ }

	void Clear()	{ times->clear(); }

	void Print()
	{
		if(process_type != ProcessType::BHLOADER)
			rcdev << lock << "***** Function Execution Times ************************************************************" << unlock;
		else
			rcdev << lock << "***** IB LOADER Function Execution Times **************************************************" << unlock;
		std::priority_queue<SortEntry, std::vector<SortEntry>, SortEntryLess > sq;
		std::map<std::string, Entry>::iterator iter = times->begin();
		char str[512] = {0};
		for(;iter != times->end(); iter++) {
			sq.push(SortEntry(iter->first, iter->second));
		}
		while(!sq.empty()) {
			sprintf(str, "%-60s \t%6llu \t%8.2fs", sq.top().n.c_str(),
					sq.top().e.call, sq.top().e.time/1000000.0);
			rcdev << lock << str << unlock;
			sq.pop();
		}


		rcdev << lock << "Number of distinct Data Packs loads: " << (uint)count_distinct_dp_loads.size() << unlock;
		rcdev << lock << "Number of distinct Data Packs decompressions: " << (uint)count_distinct_dp_decompressions.size() << unlock;
		rcdev << lock << "Size of DP read from disk: " << ((double)NoBytesReadByDPs / 1024 / 1024) << "MB" << unlock;
		rcdev << lock << "Size of uncompressed DPs: " << ((double)SizeOfUncompressedDP / 1024 / 1024) << "MB" << unlock;
		rcdev << lock << "*******************************************************************************************" << unlock;
	}

	std::map<std::string, Entry>* times;
    IBMutex fet_mutex;
	//pthread_mutex_t	cs;
	bool 			flush;		// should we flush on the next Add()?
};

class FETOperator
{
public:
	FETOperator(const std::string& identyfier)
		:id(identyfier)
	{
#ifdef __WIN__
		start_ticks = clock();
#else
		gettimeofday(&start_time, NULL);
#endif

	}

	~FETOperator()
	{

#ifdef __WIN__
		fet->Add(id, (clock() - start_ticks) * 1000);
#else
		struct timeval t2;
		_uint64 sec, usec;
		sec = usec = 0;
		gettimeofday(&t2, NULL);

		sec += (t2.tv_sec - start_time.tv_sec);
		if (t2.tv_usec < start_time.tv_usec) {
			sec--;
			t2.tv_usec += 1000000l;
		};

		usec += (t2.tv_usec - start_time.tv_usec);
		if (usec >= 1000000l)
		{
			usec -=  1000000l;
			sec++;
		};
		usec += sec * 1000000;
		fet->Add(id, usec);
#endif
	}

private:
	std::string id;
#ifdef __WIN__
	clock_t start_ticks;
#else
	struct timeval start_time, t2;
#endif
};


inline void FunctionsExecutionTimes::FlushFET(int signum)
{
	fet->Flush();
}

inline void FunctionsExecutionTimes::InstallFlushSignal()
{
	// install FlushFET as a signal handler for SIGUSR1
//	void* ret = (void*)signal(16, FunctionsExecutionTimes::FlushFET);
	signal(16, FunctionsExecutionTimes::FlushFET);
}

//#ifdef FUNCTIONS_EXECUTION_TIMES
//#define _FET(name,str)		FETOperator name(str)
//#else
//#define _FET(name,str)
//#endif



#define MEASURE_FET(X) FETOperator feto(X)

#else
	#define MEASURE_FET(X)
#endif

#endif /* not CORE_FET_H_INCLUDED */
