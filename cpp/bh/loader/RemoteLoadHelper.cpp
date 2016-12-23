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

#include <boost/bind.hpp>

#include "RemoteLoadHelper.h"
#include "system/IBPipe.h"
#include "system/IOParameters.h"
#include "system/RCSystem.h"
#include "loader/Loader.h"
#include "core/tools.h"

using namespace std;

string RemoteLoadHelper::make_pipe_path() {
#ifdef _MSC_VER
	string path( "\\\\.\\pipe\\rlp-" );
#else /* #ifdef _MSC_VER */
	string path( "/tmp/rlp-" );
#endif /* #else #ifdef _MSC_VER */
	MakeStringUnique( path );
	return ( path );
}

string RemoteLoadHelper::make_file_path() {
#ifdef _MSC_VER
	char temp_path[4096];
	GetTempPath(4096, temp_path);
	string path( string(temp_path) + "rlp-" );
#else /* #ifdef _MSC_VER */
	string path( "/tmp/rlp-" );
#endif /* #else #ifdef _MSC_VER */
	MakeStringUnique( path );
	return ( path );
}

RemoteLoadHelper::RemoteLoadHelper(IOParameters& iop)
	: is_remote(iop.LocalLoad()), infile(iop.Path())
{
#ifndef EMBEDDED_LIBRARY
	if ( is_remote ) {
		string path(make_pipe_path());
		pipe = pipe_t( new IBPipe( true, true ) );
		pipe->open( path, static_cast<IBPipe::MODE::mode_t>( IBPipe::MODE::WRITE | IBPipe::MODE::SERVER ), 30 * 1000 );
		iop.SetOutputPath(path);
	}
#endif /* #ifndef EMBEDDED_LIBRARY */
}

RemoteLoadHelper::~RemoteLoadHelper()
{
}

void RemoteLoadHelper::operator()(IBProcess& proc)
{
#ifndef EMBEDDED_LIBRARY
	if ( is_remote ) {
		FunctionExecutor fe( boost::function0<void>(), boost::bind(&IBPipe::close, pipe.get()) );
		bool first_try = true;
		while (true) {
			try {
				if (first_try) {
					first_try = false;
					pipe->open_force();
				}
				else
					pipe->retry_open_force();
				break;
			} catch (...) {
				if (!proc.Exists())
					throw InternalRCException("BHLoader process died");
			}
		}
		Push();
	}
#endif /* #ifndef EMBEDDED_LIBRARY */
}


void RemoteLoadHelper::Push()
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
#ifndef EMBEDDED_LIBRARY
	static int const BUF_SIZE = 65536;
	char buffer[BUF_SIZE];
	THD& thd( ConnectionInfoOnTLS.Get().Thd() );
	net_request_file( &thd.net, infile.c_str() );
	IO_CACHE  io;
	if ( init_io_cache( &io, -1, 0, READ_NET, 0L, 1, MYF(MY_WME) ) ) {
		throw FileRCException("Failed to initialize io cache.");
	} else {
		try {
			FunctionExecutor fe(boost::function0<void>(), boost::bind(&RemoteLoadHelper::ReadAllFromCache, this, boost::ref(io)));
			io.read_function = _my_b_net_read;
			int chr( 0 );
			size_t pos = 0;
			while ( ( chr = my_b_get( &io ) ) != my_b_EOF && thd.killed == 0) {
				buffer[pos++] = static_cast<char>( chr );
				if ( pos == BUF_SIZE ) {
					pipe->write(buffer, (long)pos);
					pos = 0;
				}
			}
			if ( pos > 0 )
				pipe->write(buffer, (long)pos);
		} catch (...) {
			::end_io_cache( &io );
			throw;
		}
		::end_io_cache( &io );
		if (thd.main_da.is_error() && thd.main_da.sql_errno()==ER_NET_READ_INTERRUPTED)
			rclog << lock << thd.main_da.message() << unlock;
	}
#endif /* #ifndef EMBEDDED_LIBRARY */
#endif
}

void RemoteLoadHelper::ReadAllFromCache(IO_CACHE& io)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	int chr( 0 );
	while ( ( chr = my_b_get( &io ) ) != my_b_EOF );
#endif
}
