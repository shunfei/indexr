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

#include <boost/noncopyable.hpp>

#include <system/ib_system.h>

#ifndef SYNCHRONIZEDVALUE_H_
#define SYNHRONIZEDVALUE_H_

template <class T>
class SynchronizedValue : public boost::noncopyable
{
public:
	SynchronizedValue(const T& value = T())
		:	value(value)
	{
	}

	virtual ~SynchronizedValue()
	{
	}

	void operator=(const T& value)
	{
		Set(value);
	}

	void Set(const T& value)
	{
		IBGuard guard(mutex);
		this->value = value;
	}

	T Get()
	{
		IBGuard guard(mutex);
		T v(value);
		return v;
	}

	bool operator==(const T& value) const
	{
		return this->value == value;
	}

	bool operator!=(const T& value) const
	{
		return this->value != value;
	}

private:
	T value;
	IBMutex mutex;
};

#endif /* SYNCHRONIZEDVALUE_H_ */
