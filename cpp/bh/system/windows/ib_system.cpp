#include <string>
#include <iostream>
#include <string.h>
#include <errno.h>

#include <boost/lexical_cast.hpp>
#include <boost/shared_array.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>

#include "system/ib_system.h"

using namespace std;
using namespace boost;

static LONGLONG freq; 

_int64 GetPid()
{
	return GetCurrentProcessId();
}

std::string GetErrorMessage(int error_id, const char* prefix)
{
    LPSTR lpMsgBuf;
    FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        error_id,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
        (LPSTR) & lpMsgBuf,
        0,
        NULL
        );
    std::string result = "";
    if (prefix) result = prefix;
    result += lpMsgBuf;
    LocalFree(lpMsgBuf);
	return result;
}

/****************************
 * IBMutex
 ****************************/
IBMutex::IBMutex(bool count_time) : m_count_time(count_time), m_time_counter(0)
{
	Init();
}

IBMutex::~IBMutex()
{
	Uninit();
}

void IBMutex::Init()
{
}

void IBMutex::Uninit()
{
}

void IBMutex::Lock()
{
	mutex.lock();
}

int IBMutex::TryTimedLock( int timeout)
{
	return mutex.timed_lock(posix_time::millisec(timeout));
}

int IBMutex::TryLock()
{
	return mutex.timed_lock(posix_time::millisec(0));
}

void IBMutex::Unlock()
{
	mutex.unlock();
}

void IBMutex::GetTime(MutexTime& mt)
{
	LARGE_INTEGER freq_struct;
	QueryPerformanceFrequency(&freq_struct);
	mt.seconds = int(m_time_counter / freq_struct.QuadPart);
	mt.miliseconds = int((m_time_counter - mt.seconds * freq_struct.QuadPart) * 1000 / freq_struct.QuadPart);
}

/****************************
 * IBCond
 ****************************/
IBCond::IBCond()
{
}

IBCond::~IBCond()
{
}

void IBCond::Wait()
{
	cond.wait(mutex.mutex);
}

void IBCond::Wait(IBMutex& m)
{
	cond.wait(m.mutex);
}

void IBCond::Signal()
{
	cond.notify_one();
}

void IBCond::Broadcast()
{
	cond.notify_all();
}

void IBCond::Lock()
{
    mutex.Lock();
}

void IBCond::Unlock()
{
    mutex.Unlock();
}

void SleepNow (int msec)
{
	::Sleep(msec);
}

/****************************
 * IBProcess
 ****************************/
IBProcess::IBProcess()
  : process(INVALID_HANDLE_VALUE)
{

}

IBProcess::~IBProcess()
{
     // should I stay or should I go?
}

void IBProcess::Start (const char* path)
{
    Start (path, 0, 0);
}

void IBProcess::Start (const char* path, const char* arg)
{
    Start (path, 1, & arg);
}

void IBProcess::Start (const char* path, const char* arg, const char* arg2)
{
    const char* pargs[] = { arg, arg2 };
    Start (path, 2, pargs);
}

void IBProcess::Start (const char* path, const char* arg, const char* arg2, const char* arg3)
{
    const char* pargs[] = { arg, arg2, arg3 };
    Start (path, 3, pargs);
}

void IBProcess::Start(const char* path, const std::vector<std::string>& args)
{
	shared_array<const char*> pargs(new const char * [args.size()]);
	transform(args.begin(), args.end(), pargs.get(), bind(&string::c_str, _1));
	Start(path, (int)args.size(), pargs.get());
}

void IBProcess::Start (const char* path, int args_count, const char* args[])
{
    if ((path == 0) || (args_count < 0)) throw IBSysException(0);

	std::string cmd_line = "\"";
	cmd_line += path;
	cmd_line += "\" ";
	for (int i=0; i < args_count; i++) {
		//cmd_line += "\"";
		cmd_line += string(args[i]) + " ";
        //cmd_line += "\" ";
	}

	PROCESS_INFORMATION pi;
    STARTUPINFOA si;

    ZeroMemory( &si, sizeof(si) );
    si.cb = sizeof(si);
    ZeroMemory( &pi, sizeof(pi) );

    BOOL ret = CreateProcessA( 0, const_cast<char*>(cmd_line.c_str()), 0, 0, FALSE, CREATE_NO_WINDOW, 0, 0, & si, & pi);
    if (!ret) throw IBSysException( GetLastError());

    CloseHandle( pi.hThread);
    process = pi.hProcess;
}

void IBProcess::Wait(int& return_code, int msec)
{
    DWORD timeout = (msec == 0) ? INFINITE : (DWORD) msec;
    DWORD ret = WaitForSingleObject( process, timeout);
    if (ret == WAIT_FAILED) {
        process = INVALID_HANDLE_VALUE;
        throw IBSysException( GetLastError());
    }

    DWORD exit_code;
    BOOL b = GetExitCodeProcess( process, & exit_code);
    if (!b) {
        process = INVALID_HANDLE_VALUE;
        throw IBSysException( GetLastError());
    }

    if (exit_code != STILL_ACTIVE) {
        process = INVALID_HANDLE_VALUE;
        if (exit_code == 0xFFFFFFFF)
            throw IBSysException(EINTR);
        return_code = (int) exit_code;
    }
}

void IBProcess::Terminate()
{
    BOOL ret = TerminateProcess( process, 0xFFFFFFFF);
    if (!ret) throw IBSysException( GetLastError());
}

PROCESS IBProcess::GetProcess()
{
    return process;
}

bool IBProcess::Exists()
{
	DWORD exit_code;
	if (!GetExitCodeProcess(process, &exit_code))
		return false;
	return (exit_code==STILL_ACTIVE);
}

PROCESS IBProcess::GetSelf()
{
    return GetCurrentProcess();
}

/****************************
 * IBThreadStorage
 ****************************/
template<class T>
IBThreadStorage<T>::IBThreadStorage()
{
    key = TlsAlloc();
}

template<class T>
IBThreadStorage<T>::~IBThreadStorage()
{
    TlsFree(key);
}

template<class T>
void IBThreadStorage<T>::Set(T& obj)
{
    if(!TlsSetValue(key, &obj))
    	throw IBSysException( GetLastError());
}

template<class T>
T& IBThreadStorage<T>::Get()
{
	void* ptr = TlsGetValue(key);
	if(!ptr)
		throw IBSysException(errno);
	return *static_cast<T*>(ptr);
}

template<class T>
bool IBThreadStorage<T>::IsValid()
{
	void* ptr = TlsGetValue(key);
	if(!ptr)
		return false;
	return true;
}

template<class T>
void IBThreadStorage<T>::Release()
{
	if(!TlsSetValue(key, NULL))
		throw IBSysException( GetLastError());
}


#include "system/ConnectionInfo.h"
template class IBThreadStorage<ConnectionInfo>;


