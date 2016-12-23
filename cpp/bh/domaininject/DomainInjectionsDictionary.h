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

#ifndef DOMAININJECTIONSDICTIONARY_H_
#define DOMAININJECTIONSDICTIONARY_H_

#include <map>
#include <string>
#include <boost/shared_ptr.hpp>
#include "system/ib_system.h"

class DomainInjectionsDictionary
{
private:
	typedef std::pair<std::string, bool> rule_t;
	typedef std::map<std::string, std::string> dictionary_t;
	typedef std::map<std::string, rule_t> columns_t;
	typedef std::map<std::string, columns_t> tables_t;
	typedef std::map<std::string, tables_t> databases_t;
	time_t _modified;
	dictionary_t _dictionary;
	databases_t _dictMap;
	std::string _dataDir;

	bool dictionary_present;
	bool columns_present;

	IBMutex mutex;

public:
	std::string get_rule( std::string const&, std::string const&, std::string const& );
	void remove_rule( std::string const&, std::string const&, std::string const& column_ = std::string() );

	void SetDataDir(const std::string& );

private:
	DomainInjectionsDictionary();
	bool is_refresh_required( void ) const;
	void refresh(); //reload dictionary from file
	bool AddDecomposer( const std::string& id, const std::string& rule, dictionary_t& );
	void LoadDecompositionsDictionary( dictionary_t& );
	void LoadDecompositionsMap( databases_t&, dictionary_t const& );

public:
	static DomainInjectionsDictionary& Instance() { return instance; }
	static void InvalidateTables(THD* thd);

private:
	static DomainInjectionsDictionary instance;

};

#endif /* DOMAININJECTIONSDICTIONARY_H_ */
