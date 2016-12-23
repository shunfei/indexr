/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

/** \file system/ib_system.h
 *  \brief Collection of functions and classes to provide portable access to system resources
 *
 */

#ifndef IB_SYSTEM_H
#define IB_SYSTEM_H

#include <vector>
#include <string>
#include <memory>
#include <sstream>
#pragma warning( disable : 4267 )
#include <boost/thread.hpp>
#pragma warning( default : 4267 )

#ifndef __GNUC__
#include <time.h>
#else
#include <sys/time.h>
#endif
#include <stdio.h>

#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>

#ifndef __GNUC__
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#endif

#ifdef __GNUC__
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
typedef int                 IBFILE;
typedef pthread_mutex_t     MUTEX;
typedef sem_t*     			NAMED_MUTEX;
typedef pthread_cond_t      CONDVAR;
typedef pthread_t           THREADVAR;
typedef pid_t               PROCESS;
typedef pthread_key_t       THREAD_KEY;
typedef void*               DWORD;
#define WINAPI
#else
#pragma warning (disable: 4290) // VC++ 8 compiler warning "C++ exception specification ignored"
#include <winsock2.h>
#include <windows.h>
typedef HANDLE              IBFILE;
typedef boost::recursive_timed_mutex   MUTEX;
typedef HANDLE              NAMED_MUTEX;
typedef boost::condition_variable_any  CONDVAR;
typedef HANDLE              THREADVAR;
typedef HANDLE              PROCESS;
typedef HANDLE              pid_t;
typedef DWORD               THREAD_KEY;
#endif

#include "RCException.h"
#include "common/CommonDefinitions.h"

#ifdef __GNUC__
#define IBSLEEP(t)          sleep(t)
#elif defined(__WIN__)
#define IBSLEEP(t)          Sleep(t*1000)
#endif

#ifdef __WIN__
typedef clock_t             IBTIMEVAL;
inline IBTIMEVAL GetCurrentTimeVal()
{
	return clock();
}
inline float GetTimeDiffWithCurrent(IBTIMEVAL t1)
{
	return float (clock() - t1) / CLOCKS_PER_SEC;
}
#else
typedef struct timeval       IBTIMEVAL;
inline IBTIMEVAL GetCurrentTimeVal()
{
	IBTIMEVAL t;
	gettimeofday(&t, NULL);
	return t;
}
inline float GetTimeDiffWithCurrent(IBTIMEVAL t1)
{
	float sec = 0;
	_uint64 usec = 0;
	IBTIMEVAL t2;
	gettimeofday(&t2, NULL);

	sec += (t2.tv_sec - t1.tv_sec);
	if (t2.tv_usec < t1.tv_usec) {
		sec--;
		t2.tv_usec += 1000000l;
	};

	usec += (t2.tv_usec - t1.tv_usec);
	if (usec >= 1000000l)
	{
		usec -= 1000000l;
		sec++;
	};
	//usec += sec * 1000000;
	sec += (float)usec/1000000l;
	return sec;
}
#endif

inline std::string sTimeNow()
{
	time_t curtime= time(NULL);
	struct tm* cdt= localtime(&curtime);
	char sdatetime[20]= "";
	std::string stime;
	sprintf(sdatetime, "%02d:%02d:%02d", cdt->tm_hour, cdt->tm_min, cdt->tm_sec);
	stime = sdatetime;
	stime += " ";
	return stime;
}

_int64 GetPid();


/** \fn std::string GetErrorMessage(int error_id, const char* prefix=0)
 * \brief Returns human readable string with a description of a system error
 *
 * \param error_id  Numerical identificator of a system error (errno or GetLastError())
 * \param prefix    Text to put before a system error description
 */
std::string GetErrorMessage(int error_id, const char* prefix=0);

struct CodeInfo
{
    CodeInfo(const char* _module, const int _line, const char* _func)
        : module(_module), line(_line), func(_func) {}
    CodeInfo(const CodeInfo& rhs)
        : module(rhs.module), line(rhs.line), func(rhs.func) {}
    const char* module;
    const int   line;
    const char* func;
};
#define CODE_INFO  CodeInfo(__FILE__, __LINE__, __FUNCTION__)

// TODO: inherits from some RCException
class IBSysException : public RCException
{
public:
    IBSysException(int err_code) : RCException(GetErrorMessage(err_code)), err(err_code) {}
    IBSysException(const IBSysException& rhs) : RCException(GetErrorMessage(rhs.err)), err(rhs.err) {}
    int GetErrorCode() const { return err; }
private:
    int err;
};

class Lockable
{
public:
	virtual ~Lockable() {}
	virtual void Lock() = 0;
	virtual void Unlock() = 0;
};

/** \class IBMutex
 *  \brief Implements a thread lock/unlock interface
 *
 *  Use instances of this class to protect shared resources
 *  for concurrent access by multiple threads.
 */

class IBMutex : public Lockable
{
    friend class IBCond;
public:
	struct MutexTime {
		int seconds;
		int miliseconds;
	};

	IBMutex(bool count_time = false);
    virtual ~IBMutex();

    /** \brief Initializing of the class instance without constructor
     */
    void Init();

    /** \brief Uninitializing of the class instance without destructor
     */
    void Uninit();

    /** \brief Get exclusive access to shared resources on the current thread
     */
    void Lock();

    /** \brief Try to get exclusive access to shared resources on the current thread
     *         in a specified time period
     *  \param timeout   a number of msec thread tries to acquire the lock
     *  \return If successful, returns true. Otherwie, false
     */
    int TryTimedLock( int timeout);

    /** \brief Try to get exclusive access to shared resources on the current thread
     *  \return If successful, returns true. Otherwie, false
     */
    int TryLock();

    /** \brief Release shared resources from the exclusive access on the current thread
     */
    void Unlock();

	void GetTime(MutexTime& mt);

private:
    MUTEX mutex;
	const bool m_count_time;
	int64 m_time_counter;
};

/** \class IBGuard
 *  \brief A helper class, which uses IBMutex instance
 *         to protect access to shared resources in a given scope
 */
class IBGuard
{
public:
    /**\brief Constructor locks a mutex
     * \param mutex  A mutex to be locked
     */
    IBGuard(Lockable& lock) : lock(lock) { lock.Lock(); }
    virtual ~IBGuard() { lock.Unlock(); }

private:
	Lockable& lock;
};

/** \class IBCond
 *  \brief Implements conditional variable interface
 *
 * IBCond implements conditional variable interface united
 * with required mutex. It is enough to have one IBCond
 * instance instead of pthread_cond_t and pthread_mutex_t.
 * Some advanced scenarios are not supported. General usage
 * make it simpler and more reliable.
 */
class IBCond
{
public:
    IBCond();
    virtual ~IBCond();

    /** \brif Get exclusive access to shared resources on the current thread
     */
    void Lock();

    /** \brief Release shared resources from the exclusive access on the current thread
     */
    void Unlock();

    /** \brief Waits for conditions to change and another thread to signal about it
     *
     * ATTENTION: due to specifics of Windows implemenation, usage must be like this
     *    while(!cond) v.Wait();
     * Usual pthread_cond_* requires only "if" instead of "while"
     */
    void Wait();
    void Wait(IBMutex&);

    /** \brief Wake up one of the waiting threads to check the changed condition
     */
    void Signal();

    /** \breif Wake up all waiting threads to check the changed condition
     */
    void Broadcast();

private:
    IBMutex mutex;
    CONDVAR cond;
};

void SleepNow(int msec);
inline int long get_raw_thread_id( boost::thread::id id_ ) {
	std::stringstream ss;
	ss << id_;
	int long id( 0 );
	ss >> std::hex >> id;
	return ( id );
}

/** \class IBProcess
 *  \brief Provides interface to start a process and wait for its completion
 */
class IBProcess
{
public:
    IBProcess();
    IBProcess(PROCESS process_) : process(process_) {}
    virtual ~IBProcess();

    /** Helper functions for convinience
     */
    void Start(const char* path);
    void Start(const char* path, const char* arg);
    void Start(const char* path, const char* arg, const char* arg2);
    void Start(const char* path, const std::vector<std::string>& args);
    void Start(const char* path, const char* arg, const char* arg2, const char* arg3);

    /** \brief Start a new process and execute a program specified by path parameter
     */
    void Start(const char* path, int args_count, const char* args[]);

    /** \brief Wait for a process to finish
     *  \param return_code  Out parameter. Value of the process's exit code
     *  \param msec         Timeout in milliseconds. Default value is 0 means infinite timeout
     */
    void Wait(int& return_code, int msec=0);

    /** \brief Terminates the created process
     */
    void Terminate();

    /** \brief Returns a process id of the created process
     */
    PROCESS GetProcess();

    /** \brief Tests the existence of the process
     */
    bool Exists();

    /** \brief Returns a process id of the calling process
     */
    static PROCESS GetSelf();

private:
    PROCESS process;
};

/** \class IBThreadStorage
 *  \brief Thread-local storage
 *  \usage Create a global IBThreadStorage instance for each pointer you want to access on multiple
 *         threads. For example, "IBThreadStorage ConnectionInfoStorage" global object accessible
 *         from everywhere. In the beginning (up on the thread call stack) call ConnectionInfoStorage.Set(pInfo)
 *         and later (down the chain of calls) get it by
 *                  ConnectionInfo* pInfo = static_cast<ConnectionInfo*>(ConnectionInfoStorage.Get());
 */
template<class T>
class IBThreadStorage
{
public:
    IBThreadStorage();
    ~IBThreadStorage();

    /** \brief Set a value for this thread
     */
    void Set(T& obj);

    /** \brief Get a value for this thread
     */
    T& Get();

    T* operator->() { return &Get(); }

    /** \brief Return true if thread is accessible
     */
	bool IsValid();
	void Release();

private:
    THREAD_KEY key;
};

#endif /* IB_SYSTEM_H */

