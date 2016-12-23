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

#ifndef GNU_CXX_EXTENDED
#define GNU_CXX_EXTENDED

#ifdef __GNUC__
#include <ext/hash_set>

// Also extend __gnu_cxx
namespace __gnu_cxx
{
#if defined(__ia64__) || defined(__x86_64__)
   // Note: Since the return value must be fit in size_t data type
   // The following has function is valid only in 64 bit linux OS, because
   // sizeof(long int) = sizeof(long long int) = sizeof(size_t)
   // where in 32 bit linux OS we have disimilarity
   // sizeof(long int) != sizeof(long long int) != sizeof(size_t)
   template<>
    struct hash<long long int>
    {
      size_t
      operator()(long long int __x) const
      { return __x; }
    };
   template<>
    struct hash<unsigned long long int>
    {
      size_t
      operator()(unsigned long long __x) const
      { return __x; }
    };
#else
   // Same as in case x86_64, but with a cast
   template<>
    struct hash<long long int>
     {
      size_t
	operator()(long long int __x) const
      { return static_cast<size_t>(__x); }
     };
   template<>
    struct hash<unsigned long long>
     {
      size_t
	operator()(unsigned long long __x) const
      { return static_cast<size_t>(__x); }
     };
#endif

  template<>
    struct hash<std::string>
    {
      size_t
      operator()(const std::string& __x) const
      { return hash< const char* >()( __x.c_str() ); }
    };
    
    template<>
      struct hash<void*>
      {
        size_t
        operator()(const void* __x) const
        { return hash<size_t>()( (size_t) __x); }
      };
    
}
#endif
#endif

