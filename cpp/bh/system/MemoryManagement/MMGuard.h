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


#ifndef MMGuard_H_
#define MMGuard_H_


class TrackableObject;

template <typename T>
class MMGuard;

template <typename T>
class MMGuardRef
{
private:
	explicit MMGuardRef(T* data, TrackableObject& owner,  bool call_delete = true) : data(data), owner(&owner), call_delete(call_delete) {}
	T* data;
	TrackableObject* owner;
	bool call_delete;
	friend class MMGuard<T>;
};

template<typename T>
struct debunk_void
{
	typedef T& type;
};

template<>
struct debunk_void<void>
{
	typedef void* type;
};

// Friend class of TrackableObject that can use MM functions that do accounting
class TrackableAccounting {
public:
	TrackableAccounting(TrackableObject *o) : owner(o) {}
	virtual ~TrackableAccounting() {}
protected:
	
	void dealloc(void *p) {
		owner->dealloc(p);
	}
	
	TrackableObject* owner;
};

template <typename T>
class MMGuard : public TrackableAccounting
{
public:
	MMGuard() : TrackableAccounting(0), data(0), call_delete(false) {}

	explicit MMGuard(T* data, TrackableObject& owner, bool call_delete = true) : TrackableAccounting(&owner), data(data), call_delete(call_delete) {}

	MMGuard(MMGuard& mm_guard) : TrackableAccounting(mm_guard.owner), data(mm_guard.data), call_delete(mm_guard.call_delete)
	{
		mm_guard.call_delete = false;
	}

	virtual ~MMGuard()
	{
		reset();
	}

	MMGuard& operator=(MMGuardRef<T> const& mm_guard)
	{
		reset();
		data = mm_guard.data;
		owner = mm_guard.owner;
		call_delete = mm_guard.call_delete;
		return *this;
	}

	MMGuard& operator=(MMGuard<T>& mm_guard)
	{
		if(&mm_guard != this) {
			reset();
			data = mm_guard.data;
			owner = mm_guard.owner;
			call_delete = mm_guard.call_delete;
			mm_guard.call_delete = false;
		}
		return *this;
	}

	operator MMGuardRef<T>()
	{
		MMGuardRef<T> mmr(data, *owner, call_delete);
		call_delete = false;
		return ( mmr );
	}

	typename debunk_void<T>::type operator*() const
	{
		return *data;
	}

	T* get() const
	{
		return data;
	}

	T* operator->() const
	{
		return data;
	}

	typename debunk_void<T>::type operator[] (uint i) const
	{
		return data[i];
	}

	T* release()
	{
		T* tmp = data;
		data = 0;
		owner = 0;
		call_delete = false;
		return tmp;
	}

	void reset()
	{
		if(call_delete && data)
			dealloc(data);
		data = 0;
		owner = 0;
		call_delete = false;
	}

private:
	T* data;
	bool call_delete;
};

#endif /* MMGuard_H_ */
