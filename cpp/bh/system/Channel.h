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

#ifndef _SYSTEM_CHANNEL_H_
#define _SYSTEM_CHANNEL_H_

#include <exception>
#include "ChannelOut.h"
#include "system/ib_system.h"

class Channel
{
public:
	Channel(bool time_stamp_at_lock = false);
	Channel(ChannelOut* output, bool time_stamp_at_lock = false);
	~Channel(void);

	void addOutput(ChannelOut* output);
	void setOn() { m_bEnabled = true; };
	void setOff() { m_bEnabled = false; };
	void setTimeStamp(bool _on = true)	{ m_bTimeStampAtLock = _on; };
	void setResourceMon(bool _on = true)    { m_bResourceMonAtLock = _on; };
	bool isOn();
	Channel& lock(uint optional_sess_id = 0xFFFFFFFF);
	Channel& unlock();

	Channel& operator<<(short value);
	Channel& operator<<(int value);

	Channel& operator<<(float value);
	Channel& operator<<(double value);
	Channel& operator<<(long double value);

	Channel& operator<<(unsigned short value);
	Channel& operator<<(unsigned int value);
	Channel& operator<<(unsigned long value);

	Channel& operator<<(int long value );
	Channel& operator<<(int long long value);
	Channel& operator<<(unsigned long long value);

	Channel& operator<<(const char* buffer);
	Channel& operator<<(const wchar_t* buffer);
	Channel& operator<<(char c);
	Channel& operator<<(const std::string& str);

	Channel& operator<<(const std::exception& exc);
	Channel& operator<<(Channel& (*_Pfn)(Channel&)) { return _Pfn(*this); };
	Channel& flush();

private:
	ChannelOut** m_Outputs;
	int m_nNoOfOutputs;
	bool m_bEnabled;
	bool m_bEnabled_tmp;
    IBMutex  channel_mutex;
	//pthread_mutex_t m_synchr;
	//char m_CurrentDate[256], m_CurrentTime[256];
	bool m_bTimeStampAtLock;
	bool m_bResourceMonAtLock;
};

inline Channel& lock(Channel& report)
{
	return report.lock();
}

inline Channel& unlock(Channel& report)
{
	report << '\n';
	report.flush();
	return report.unlock();
}

inline Channel& flush(Channel& report)
{
	return report.flush();
}

inline Channel& endl(Channel& report)
{
	report << '\n';
	return report.flush();
}

#endif //_SYSTEM_CHANNEL_H_

