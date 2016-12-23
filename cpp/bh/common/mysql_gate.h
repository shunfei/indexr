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

#ifndef MYSQL_GATE_H_INCLUDED
#define MYSQL_GATE_H_INCLUDED 1

#include <vector>
#include <string>

extern const std::string CURRENT_SERVER_VERSION;

#ifdef _MSC_VER
#undef ETIMEDOUT
#endif

#ifdef PURE_LIBRARY

#ifdef _MSC_VER

#undef strcasecmp

#include "config-win.h"

#if !defined(HAVE_ULONG) && !defined(__USE_MISC)
typedef unsigned long	ulong;		  /* Short for unsigned long */
#endif

#undef max
#undef min
#undef bool
#undef test
#undef sleep

#undef pthread_mutex_init
#undef pthread_mutex_lock
#undef pthread_mutex_unlock
#undef pthread_mutex_destroy
#undef pthread_mutex_wait
#undef pthread_mutex_timedwait
#undef pthread_mutex_trylock
#undef pthread_mutex_t
#undef pthread_cond_init
#undef pthread_cond_wait
#undef pthread_cond_timedwait
#undef pthread_cond_t
#undef pthread_attr_getstacksize

#endif

#endif

#ifndef PURE_LIBRARY

#undef strcasecmp

#include "mysql_priv.h"
#include "sql_select.h"
#include "my_pthread.h"
#include "sp_rcontext.h"
#include "sql_repl.h"
#include "mysql_com.h"
#include "mysqld_suffix.h"

/* Putting macros named like `max', `min' or `test'
 * into a header is a terrible idea. */

#undef max
#undef min
#undef bool
#undef test
#undef sleep

#undef pthread_mutex_init
#undef pthread_mutex_lock
#undef pthread_mutex_unlock
#undef pthread_mutex_destroy
#undef pthread_mutex_wait
#undef pthread_mutex_timedwait
#undef pthread_mutex_trylock
#undef pthread_mutex_t
#undef pthread_cond_init
#undef pthread_cond_wait
#undef pthread_cond_timedwait
#undef pthread_cond_t
#undef pthread_attr_getstacksize

typedef MYSQL_TIME TIME;

typedef std::vector<enum_field_types> fields_t;

typedef volatile THD::killed_state ThreadKilledState_T;

int wildcmp(const DTCollation& collation, const char *str, const char *str_end, const char *wildstr,const char *wildend, int escape, int w_one, int w_many);
size_t strnxfrm(const DTCollation& collation, uchar* src, size_t src_len, const uchar* dest, size_t dest_len);

extern my_time_t sec_since_epoch_TIME(MYSQL_TIME *t);

#else

struct CHARSET_INFO {
		int mbmaxlen;
		const char* csname;
		const char* name;
};

class DTCollation
{
public:
	DTCollation()
	{
		collation = new CHARSET_INFO();
		collation->mbmaxlen = 1;
		collation->csname = 0;
		collation->name = 0;
		derivation = 0;
	}

	DTCollation(const DTCollation& dtc)
	{
		collation = new CHARSET_INFO();
		collation->mbmaxlen = 1;
		collation->csname = 0;
		collation->name = 0;
		derivation = 0;
	}

	DTCollation& operator=(const DTCollation& dtc)
	{
		derivation = dtc.derivation;
		return *this;
	}

	~DTCollation()
	{
		delete collation;
	}

	void set(CHARSET_INFO* ci)
	{
	}

	CHARSET_INFO* collation;
	int derivation;
};

class select_export {
};

const char* const InfobrightServerVersion();

#ifndef PURE_LIBRARY
typedef int Item_result;
typedef int enum_field_types;
typedef std::vector<enum_field_types> fields_t;
struct MYSQL_TIME;
#else
#include "mysql.h"
typedef std::vector<enum_field_types> fields_t;
#endif

class THD {
public:
	THD()
		:	killed(NOT_KILLED)
	{
	}
	const static int NOT_KILLED = 0;
	int		killed;
};

typedef unsigned long long int ulonglong;
typedef long long int	longlong;


typedef int ThreadKilledState_T;

typedef unsigned long ha_rows;

typedef long my_time_t;

struct TABLE;
class THD;
class Item;
class Item_func;
class select_result;
struct handlerton;
class Item_field;
template <class T> class List;
class sql_exchange;
struct LEX;
class SELECT_LEX_UNIT;
class my_decimal;
class SELECT_LEX;
struct st_table;
class String;
struct TABLE_LIST;
struct LEX_STRING;
class COND;
class Item_equal;
struct ORDER;
template <class T> class List_iterator_fast;
struct SQL_LIST;
class Field;

struct TABLE_SHARE;

int wildcmp(const DTCollation& collation, const char *str,const char *str_end, const char *wildstr,const char *wildend, int escape,int w_one, int w_many);
size_t strnxfrm(const DTCollation& collation, unsigned char* src, size_t src_len, const unsigned char* dest, size_t dest_len);

#endif
#endif /* not MYSQL_GATE_H_INCLUDED */

