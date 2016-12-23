/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include "common/stdafx.h"
#if defined(__linux__)
#include <fcntl.h>
#elif defined(__sun__)
#include <sys/swap.h>
#include <unistd.h>
#include <procfs.h>
#include <fcntl.h>
#elif defined(__WIN__)
#include <winsock2.h>
#include <windows.h>
#include <psapi.h>
#pragma comment(linker, "/DEFAULTLIB:psapi.lib")
#endif
#include <stdlib.h>
#include <stdio.h>
#include <sstream>
#include "SystemInformation.h"

#if defined(__linux__)
int sscanf_meminfo(const char* str, const char* start, long long *value)
{
    const char *pt1 = strstr(str, start);
    if (pt1) {
        char *pend;
        pt1 += strlen(start);
        *value = strtol (pt1, &pend, 10);
        return 0;
    }
    printf("Could not parse mem info.\n");
    return 1;
}
#endif

#if defined(__sun__)

#define MAXSTRSIZE 80

int swappages(long &swappage_total, long &swappage_free)
{
    swaptbl_t      *s;
    int            i, n, num;
    char           *strtab;    /* string table for path names */
	
    swappage_total = 0;
    swappage_free = 0;

    if ((num = swapctl(SC_GETNSWP, 0)) == -1)
        return 1; //ignore
    if (num == 0)
        return 0;  //ignore
    /* allocate swaptable for num+1 entries */
    if ((s = (swaptbl_t *)
        malloc(num * sizeof(swapent_t) + 
            sizeof(struct swaptable))) ==
        (void *) 0) {
        fprintf(stderr, "malloc Failed.\n");
        return 1;
    }
    /* allocate num+1 string holders */
    if ((strtab = (char *)
        malloc((num + 1) * MAXSTRSIZE)) == (void *) 0) {
		free(s);
        fprintf(stderr, "malloc Failed.\n");
        return 2;
    }
    /* initialize string pointers */
    for (i = 0; i < (num + 1); i++) {
        s->swt_ent[i].ste_path = strtab + (i * MAXSTRSIZE);
    }
   
    s->swt_n = num + 1;
    if ((n = swapctl(SC_LIST, s)) < 0) {
		free(s);
		free(strtab);
        fprintf(stderr, "swapctl Failed.\n");
        return 1;
    }

    for (i = 0; i < n; i++) {
//      printf("%s %ld %ld\n",
//         s->swt_ent[i].ste_path, s->swt_ent[i].ste_pages, s->swt_ent[i].ste_free);
        swappage_total += s->swt_ent[i].ste_pages;
        swappage_free += s->swt_ent[i].ste_free; 
    }
    free(s);
    free(strtab);
    return 0;
}
#endif

int GetSystemMemInfo(IBMemInfo &minfo)
{
    memset (&minfo, 0, sizeof(IBMemInfo));

#if defined(__linux__)
    int          memfd;
    char         buff[1024];
    ssize_t      nread;

    if ((memfd = open("/proc/meminfo", O_RDONLY, 0)) == -1) {
        printf("Could not open /proc/meminfo.\n");
        return -1;
    }
    if ((nread = read(memfd, buff, 1023)) <= 0) {
        printf("Could not read /proc/meminfo.\n");
        close(memfd);
        return -1;
    }
    close(memfd);

    sscanf_meminfo(buff, "MemTotal:", &minfo.MemTotal);
    sscanf_meminfo(buff, "MemFree:",  &minfo.MemFree);
    sscanf_meminfo(buff, "Cached:",   &minfo.MemCached);
    sscanf_meminfo(buff, "SwapTotal:",&minfo.SwapTotal);
    sscanf_meminfo(buff, "SwapFree:", &minfo.SwapFree);

    minfo.VTotal = minfo.MemTotal + minfo.SwapTotal;
    minfo.VFree  = minfo.MemFree  + minfo.SwapFree;
#elif defined(__sun__)
    long long system_pages    = sysconf(_SC_PHYS_PAGES);
    long long system_avpages  = sysconf(_SC_AVPHYS_PAGES);
    long long system_pagesize = sysconf(_SC_PAGESIZE);

//    long long swap_pages = 0;
//    long long swap_free  = 0;

    minfo.MemTotal  = (system_pages   * system_pagesize) / 1024;
    minfo.MemFree   = (system_avpages * system_pagesize) / 1024;

    minfo.MemCached = 0;

    long swappage_total, swappage_free;

    swappages(swappage_total, swappage_free);

    minfo.SwapTotal = (swappage_total * system_pagesize) / 1024;
    minfo.SwapFree  = (swappage_free  * system_pagesize) / 1024;

    minfo.VTotal = minfo.MemTotal + minfo.SwapTotal;
    minfo.VFree  = minfo.MemFree  + minfo.SwapFree;
#elif defined(__WIN__)
    MEMORYSTATUSEX statex;
    statex.dwLength = sizeof (statex);
 
    GlobalMemoryStatusEx(&statex);

    minfo.MemTotal  = statex.ullTotalPhys/1024;
    minfo.MemFree   = statex.ullAvailPhys/1024;
    minfo.MemCached = 0;
    minfo.SwapTotal = statex.ullTotalPageFile/1024;
    minfo.SwapFree  = statex.ullAvailPageFile/1024;
    minfo.VTotal = statex.ullTotalVirtual/1024;
    minfo.VFree  = statex.ullAvailVirtual/1024;
#endif
    return 0;
}

// Some wrappers
////////////////////////////////////////////////

unsigned long long GetSystemFreeMemory()
{
    IBMemInfo minfo;
    GetSystemMemInfo(minfo);
    return minfo.MemFree + minfo.MemCached;
}

unsigned long long GetSystemVirtualMemory()
{
    IBMemInfo minfo;
    GetSystemMemInfo(minfo);
    return minfo.VTotal;
}

unsigned long long GetProcessVirtualMemory()
{
#if defined(__linux__)
    char stat_file[64]={0};
    int fd;
    const int buffer_size = 1024;
    char buffer[buffer_size];
    int read_size;

    sprintf(stat_file, "/proc/%d/stat", getpid());
    fd = open(stat_file, O_RDONLY);
    if (fd >= 0) {
        if ((read_size = read(fd, buffer, buffer_size)) == -1)
            return 0;
        close(fd);
    }

    std::string statistics(std::string(buffer, read_size));
    // The closing bracket ')' before the R (running flag) is the last static
    // point of reference in the stats line.  The vmem size field is 21 fields
    // to the right of that ')'. Refer to the 'proc' man pages more information. 
    std::stringstream tokens(statistics.substr(statistics.find_last_of(')') + 1));
    std::string token;
    for (size_t position = 21; position != 0; --position ) {
        if (!(tokens >> token))
               return 0;
	}
    return strtoull(token.c_str(), NULL, 10);
#elif defined(__sun__)
    int fd;
    char psinfo_file[64]={0};
	psinfo_t psinfo;

    sprintf(psinfo_file, "/proc/%d/psinfo", getpid());
    fd = open(psinfo_file, O_RDONLY);
    if (fd >= 0) {
        if (read(fd, &psinfo, sizeof(psinfo_t)) != -1) {
			close(fd);
            return psinfo.pr_size;
        }
		close(fd);
    }
    return 0;
#elif defined(__WIN__)
    PROCESS_MEMORY_COUNTERS_EX pmc;
    GetProcessMemoryInfo(GetCurrentProcess(), (PROCESS_MEMORY_COUNTERS*)&pmc, sizeof(pmc));
    return pmc.PrivateUsage;
#endif
    return 0;
}

void PrintMemoryInfo()
{
    IBMemInfo minfo;
    GetSystemMemInfo(minfo);
    printf("===system memory information====\n");
    #if defined(__linux__) || defined(__sun__)
    printf("pid=%d\n", getpid());
    #endif
    printf("MemTotal=%lld\nMemFree=%lld\n", minfo.MemTotal, minfo.MemFree);
    printf("Cached=%lld\nSwapTotal=%lld\nVTotal=%lld\n", minfo.MemCached, minfo.SwapTotal, minfo.VTotal);
    printf("SystemFreeMemory=%lld\nSystemVirtualMemory=%lld\n", GetSystemFreeMemory(), GetSystemVirtualMemory());
    char *testalloc = (char*)malloc(10*1024*1024);  // allocate some memory to have some virtual memory
    printf("ProcessVirtualMemory=%lld\n", GetProcessVirtualMemory());
    printf("=================================\n");
    free(testalloc);
}

#if defined(TESTSYSTEMMEMINFO)
int main()
{
    while (true) {
        PrintMemoryInfo();
        #if defined(__linux__) || defined(__sun__)
        sleep(5);
        #elif defined(__WIN__)
        Sleep(5000);
        #endif
    }
    return 0;
}
#endif
