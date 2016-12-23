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


#ifndef LOCAL_FWD_H_
#define LOCAL_FWD_H_

#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

#define USE_THIS_MACRO_TO_DO_RC_TABLE_A_FRIEND friend class RCTableImpl;

enum	ExternalDataFormat {TXT_VARIABLE};
typedef ExternalDataFormat EDF;

class RCTableImpl;

typedef RCTableImpl RCTable;

template<typename T> class NullaryFunctionObject;
class IOParameters;

class DataLoaderImpl;
typedef DataLoaderImpl DataLoader;

#endif /* LOCAL_FWD_H_ */
