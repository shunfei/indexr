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

#include <iostream>
#include <fstream>
#include <algorithm>

#include <boost/tokenizer.hpp>

#include "common/CommonDefinitions.h"
#include "DomainInjectionsDictionary.h"
#include "Halver.h"
#include "IPv4_Decomposer.h"
#include "IPv4_Decomposer2.h"
#include "Concatenator.h"

DomainInjectionsDictionary DomainInjectionsDictionary::instance;

using namespace std;
using namespace boost;

#define INFOBRIGHT_SYSTEM_DATABASE "sys_infobright"
#define INFOBRIGHT_DECOMPOSITIONS_DICTIONARY_TABLE  "sys_infobright/decomposition_dictionary.CSV"
#define INFOBRIGHT_COLUMNS_TABLE  "sys_infobright/columns.CSV"

extern char bh_sysvar_refresh_sys_infobright;


DomainInjectionsDictionary::DomainInjectionsDictionary()
	: _modified( 0 ), _dictionary(), _dictMap(), dictionary_present(false), columns_present(false)
{
}

void DomainInjectionsDictionary::SetDataDir(const std::string& dataDir_ )
{
	_dataDir = dataDir_;
}

void DomainInjectionsDictionary::refresh()
{
	BHASSERT( !_dataDir.empty(), "datadir not set" );
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(is_refresh_required());

	time_t currMod (0);

	if(DoesFileExist(_dataDir + INFOBRIGHT_COLUMNS_TABLE)) {
		columns_present = true;
		currMod = GetFileTime( _dataDir + INFOBRIGHT_COLUMNS_TABLE );
	} else
		columns_present = false;

	if(DoesFileExist(_dataDir + INFOBRIGHT_DECOMPOSITIONS_DICTIONARY_TABLE)) {
		currMod = max(currMod, GetFileTime(_dataDir + INFOBRIGHT_DECOMPOSITIONS_DICTIONARY_TABLE));
		dictionary_present = true;
	} else
		dictionary_present = false;

	_modified = currMod;

	dictionary_t dictionary;
	LoadDecompositionsDictionary( dictionary );

	databases_t dictMap;
	LoadDecompositionsMap( dictMap, dictionary );

	using std::swap;
	swap( _dictionary, dictionary );
	swap( _dictMap, dictMap );

	

	bh_sysvar_refresh_sys_infobright = FALSE;
}

void DomainInjectionsDictionary::LoadDecompositionsMap( databases_t& db_, dictionary_t const& dictionary_ )
{
	int const COLUMN_COUNT( 4 );
	string columnsPath( _dataDir + INFOBRIGHT_COLUMNS_TABLE );
	ifstream fin( columnsPath.c_str() );
	if (!fin.is_open()) {
		rclog << lock << "Warning: An attempt to load decompositions dictionary map failed. " << unlock;
	} else {
		typedef escaped_list_separator<char> tokenizer_function_t;
		typedef tokenizer<tokenizer_function_t> tokenizer_t;
		typedef vector<string> tokens_t;
		string line;

		tokenizer_t tokenizer( line );
		tokens_t tokens;
		while ( ! getline( fin, line, '\n' ).fail() ) {
			tokenizer.assign( line );
			tokens.assign( tokenizer.begin(), tokenizer.end() );
			if ( tokens.size() != COLUMN_COUNT )
				rclog << lock << "Warning: Failed to parse definition of decomposition map: " << line << unlock;
			else {
				typedef pair<databases_t::iterator, bool> databases_insert_result_t;
				databases_insert_result_t dir( db_.insert( make_pair( tokens[0], tables_t() ) ) );
				typedef pair<tables_t::iterator, bool> tables_insert_result_t;
				tables_insert_result_t tir( dir.first->second.insert( make_pair( tokens[1], columns_t() ) ) );
				typedef pair<columns_t::iterator, bool> columns_insert_result_t;
				columns_insert_result_t cir( tir.first->second.insert( make_pair( tokens[2], make_pair( tokens[3], false ) ) ) );
				cir.first->second.second = ( ( ! cir.second ) || ( dictionary_.find( tokens[3] ) == dictionary_.end() ) );
				if(!cir.second)
					rclog << lock << "Warning: Duplicated decompositions for column: "<< tokens[0] << "." << tokens[1] << "." << tokens[2] << unlock;
			}
		}
	}
}

void DomainInjectionsDictionary::LoadDecompositionsDictionary( dictionary_t& dictionary )
{
	string decompDictPath( _dataDir + INFOBRIGHT_DECOMPOSITIONS_DICTIONARY_TABLE );

	ifstream fin(decompDictPath.c_str());
	if (!fin.is_open()) {
		rclog << lock << "Warning: An attempt to load decompositions dictionary failed. " << unlock;

	} else {
		typedef tokenizer< escaped_list_separator<char> > Tokenizer;
		vector<string> vec;
		string line;

		while ( !getline(fin, line, '\n').fail() ) {
			Tokenizer tok(line);
			vec.assign(tok.begin(), tok.end());
			if(vec.size() != 3)
				rclog << lock << "Warning: Failed to parse definition of decomposition : " << line << unlock;
			else if(!DomainInjectionManager::IsValid(vec[1]))
				rclog << lock << "Warning: The decomposition rule '" << vec[1] << "' is invalid." << unlock;
			else if (!AddDecomposer( vec[0], vec[1], dictionary ) ) {
				rclog << lock << "Warning: Unable to initialize '" << vec[0] << "' decomposition." << unlock;
			}

		}
	}
}

bool DomainInjectionsDictionary::AddDecomposer(const std::string& id, const std::string& rule, dictionary_t& dictionary )
{
	if(dictionary.find(id) ==  dictionary.end()) {
		if (rule.compare("PREDEFINED")==0) {
			if (id.compare(Halver::GetName())==0
				|| id.compare(IPv4_Decomposer::GetName())==0
				|| id.compare(IPv4_Decomposer2::GetName())==0) {
					dictionary[id] = id;
					return true;
			}
		}
		else {
			if (!Concatenator::IsValid(rule))
				return false;
			dictionary[id] = rule;
			return true;
		}
	}
	return false;
}

std::string DomainInjectionsDictionary::get_rule( std::string const& db_, std::string const& table_, std::string const& column_ )
{
	IBGuard mutex_guard(mutex);
	if ( is_refresh_required() )
		refresh();
	databases_t::const_iterator db( _dictMap.find( db_ ) );
	string rule;
	if ( db != _dictMap.end() ) {
		/* otherwise throw DatabaseRCException( string( "no decomposition for given database: " ) + db_ ); */
		tables_t::const_iterator table( db->second.find( table_ ) );
		if ( table != db->second.end() ) {
			/* otherwise throw DatabaseRCException( string( "no decomposition for given table: " ) + db_ + "." + table_ ); */
			columns_t::const_iterator column( table->second.find( column_ ) );
			if ( column != table->second.end() ) {
				/* otherwise throw DatabaseRCException( string( "no decomposition for given column: " ) + db_ + "." + table_ + "." + column_ ); */
				if ( column->second.second )
					throw DatabaseRCException( string( "decomposition is invalid for given column: " ) + db_ + "." + table_ + "." + column_ );
				dictionary_t::const_iterator ruleIt( _dictionary.find( column->second.first ) );
				if ( ruleIt == _dictionary.end() )
					throw DatabaseRCException( string( "no such rule: " ) + column->second.first + ", for given column: " + db_ + "." + table_ + "." + column_ );
				rule = ruleIt->second;
			}
		}
	}
	return ( rule );
}

bool DomainInjectionsDictionary::is_refresh_required( void ) const
{
	return bh_sysvar_refresh_sys_infobright;
	/*string decompDictPath( _dataDir + INFOBRIGHT_DECOMPOSITIONS_DICTIONARY_TABLE );
	string columnsPath( _dataDir + INFOBRIGHT_COLUMNS_TABLE );
	time_t currMod(0);
	bool refresh = false;
	if(DoesFileExist(columnsPath)) {
		currMod = GetFileTime( columnsPath );
		if(!columns_present)
			refresh = true;
	} else if(columns_present)
		refresh = true;

	if(DoesFileExist(decompDictPath)) {
		currMod = max(currMod, GetFileTime(decompDictPath));
		if(!dictionary_present)
			refresh = true;
	} else if(dictionary_present)
		refresh = true;

	return refresh || ( _modified < currMod );*/
}

void DomainInjectionsDictionary::remove_rule( std::string const& db_, std::string const& table_, std::string const& column_ )
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	IBGuard mutex_guard(mutex);
	int const COLUMN_COUNT( 4 );
	string columnsPath( _dataDir + INFOBRIGHT_COLUMNS_TABLE );
	string tmp_columnsPath( _dataDir + INFOBRIGHT_COLUMNS_TABLE + "_tmp" );
	ifstream fin( columnsPath.c_str() );

	bool changed = false;

	if (!fin.is_open()) {
		rclog << lock << "Warning: An attempt to load decompositions dictionary map failed. " << unlock;
	} else {
		ofstream fout( tmp_columnsPath.c_str() );
		if (!fout.is_open())
			throw DatabaseRCException("Removing decomposition rule failed - unable to create " + tmp_columnsPath + " file.");
		typedef escaped_list_separator<char> tokenizer_function_t;
		typedef tokenizer<tokenizer_function_t> tokenizer_t;
		typedef vector<string> tokens_t;
		string line;

		changed = false;
		tokenizer_t tokenizer( line );
		tokens_t tokens;
		while ( ! getline( fin, line, '\n' ).fail() ) {
			tokenizer.assign( line );
			tokens.assign( tokenizer.begin(), tokenizer.end() );
			if ( tokens.size() != COLUMN_COUNT )
				rclog << lock << "Warning: Failed to parse definition of decomposition map: " << line << unlock;
			else if (!(tokens[0] == db_ && tokens[1] == table_ && (column_.empty() || tokens[2] == column_))) {
				fout << line << '\n';
			} else
				changed = true;
		}
		fout.close();
		if(changed) {
			FlushFileChanges(tmp_columnsPath);
			RenameFile(tmp_columnsPath, columnsPath);
			FlushDirectoryChanges(INFOBRIGHT_SYSTEM_DATABASE);
			bh_sysvar_refresh_sys_infobright = true;
			refresh();
			query_cache.invalidate_by_MyISAM_filename((_dataDir + INFOBRIGHT_COLUMNS_TABLE).c_str());
			TABLE_LIST tables;
			tables.init_one_table("sys_infobright", "columns", TL_READ); // TL_READ ???
#if !defined(EMBEDDED_LIBRARY)	
			close_cached_tables(current_thd, &tables, TRUE, FALSE, FALSE);
#else
			close_cached_tables(0, &tables, TRUE, FALSE, FALSE);
#endif
		} else
			RemoveFile(tmp_columnsPath);
	}
#endif
}

void DomainInjectionsDictionary::InvalidateTables(THD* thd)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	query_cache.invalidate_by_MyISAM_filename((Instance()._dataDir + INFOBRIGHT_DECOMPOSITIONS_DICTIONARY_TABLE).c_str());
	query_cache.invalidate_by_MyISAM_filename((Instance()._dataDir + INFOBRIGHT_COLUMNS_TABLE).c_str());

	TABLE_LIST tables;
	tables.init_one_table("sys_infobright", "decomposition_dictionary", TL_READ); // TL_READ ???

	TABLE_LIST columns;
	columns.init_one_table("sys_infobright", "columns", TL_READ); // TL_READ ???

	tables.next_local = &columns;

	//shouldn't open_and_lock_tables or similar be called before? 
	close_cached_tables(thd, &tables, TRUE, FALSE, FALSE);
#endif
}
