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

#ifndef _BHASSERT_H_
#define _BHASSERT_H_

#include <cassert>
#include "system/RCException.h"

struct FailedAssertion {
	std::string msg;
	FailedAssertion( std::string const& msg_ ) : msg( msg_ ) {}
	char const* what() const { return ( msg.c_str() ); }
};

#define DEBUG_COMA ,

#ifdef NDEBUG

#define BHASSERT(condition, message) do { if (!(condition)) { throw RCException(message); } } while(0)
#define BHASSERT_WITH_NO_PERFORMANCE_IMPACT(condition) assert(condition)
#else
/* Debug version of assertion macros. */
#define BHSTRINGIFY_REAL(a) #a
#define BHSTRINGIFY(a) BHSTRINGIFY_REAL(a)
#define BHASSERT(condition, message) do { if (!(condition)) { throw RCException(message); } } while(0)
#define BHASSERT_WITH_NO_PERFORMANCE_IMPACT(condition) assert(condition)
// #define BHASSERT( cond, message ) do { if ( ! ( cond ) ) { if ( getenv("BHUT") ) { throw FailedAssertion( #cond #message ); } else { assert((cond)&&(message)); } } } while( 0 )
// #define BHASSERT_WITH_NO_PERFORMANCE_IMPACT(condition) \
// 	do { \
// 		if ( ! ( condition ) ) { \
// 			if ( getenv("BHUT") ) { \
// 				std::string msg("Failed assertion: "); \
// 				msg += #condition; \
// 				msg += ", from: "; \
// 				msg += __FILE__":"; \
// 				msg += BHSTRINGIFY(__LINE__); \
// 				throw FailedAssertion(msg); \
// 			} else { \
// 				assert(condition); \
// 			} \
// 		} \
// 	} while( 0 )
#endif

#ifdef EXTRA_GUARDS
#define BHASSERT_EXTRA_GUARD( cond, message ) do { if (!(cond)) { throw RCException(message); } } while(0)
#else
#define BHASSERT_EXTRA_GUARD( cond, message ) if( ! ( cond ) ) BHASSERT_WITH_NO_PERFORMANCE_IMPACT(message)
#endif

#define BHERROR(message) BHASSERT(false, message)

#endif /*_BHASSERT_H_*/
