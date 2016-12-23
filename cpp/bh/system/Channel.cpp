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

#include "core/tools.h"
#include "system/RCSystem.h"

#include "Channel.h"

using namespace std;

Channel::Channel(bool time_stamp_at_lock)
	:	m_Outputs(NULL), m_nNoOfOutputs(0), m_bEnabled(true), m_bEnabled_tmp(true), 
	m_bTimeStampAtLock(time_stamp_at_lock), m_bResourceMonAtLock(false)
{
}

Channel::Channel(ChannelOut* output, bool time_stamp_at_lock)
	:	m_Outputs(NULL), m_nNoOfOutputs(0), m_bEnabled(true), m_bEnabled_tmp(true), 
	m_bTimeStampAtLock(time_stamp_at_lock), m_bResourceMonAtLock(false)
{
	addOutput(output);
}

Channel::~Channel(void)
{
	if(m_nNoOfOutputs > 0)
		delete[] m_Outputs;
}

void Channel::addOutput(ChannelOut* output)
{
	m_nNoOfOutputs++;
	ChannelOut** newOutputs = new ChannelOut*[m_nNoOfOutputs];
	if (m_nNoOfOutputs > 1)
	{
		for (int out = 0; out < m_nNoOfOutputs-1; out++) newOutputs[out] = m_Outputs[out];
		delete[] m_Outputs;
	}
	m_Outputs = newOutputs;
	m_Outputs[m_nNoOfOutputs-1] = output;
}

bool Channel::isOn() 
{ 
	return m_bEnabled && (!ConnectionInfoOnTLS.IsValid() || ConnectionInfoOnTLS->GetDisplayLock() == 0); 
}

Channel& Channel::lock(uint optional_sess_id)
{
	//pthread_mutex_lock(&m_synchr);
	
    channel_mutex.Lock();
	//if(m_bTimeStampAtLock && m_bEnabled /*&& optional_sess_id != 0xFFFFFFFF*/)
	if(ConnectionInfoOnTLS.IsValid() && ConnectionInfoOnTLS->GetDisplayLock() > 0) {
		m_bEnabled_tmp = m_bEnabled;
		m_bEnabled = false;		
	}
	if(m_bTimeStampAtLock && m_bEnabled /*&& optional_sess_id != 0xFFFFFFFF*/)
	{
		time_t curtime= time(NULL);
		struct tm* cdt= localtime(&curtime);
		char sdatetime[20]= "";
		sprintf(sdatetime, "%4d-%02d-%02d %02d:%02d:%02d", cdt->tm_year + 1900,
					cdt->tm_mon + 1, cdt->tm_mday, cdt->tm_hour, cdt->tm_min, cdt->tm_sec);
		(*this) << sdatetime << ' ';
		if (optional_sess_id != 0xFFFFFFFF)
			(*this) << '[' << optional_sess_id << "] ";
		// Uncomment this to see TrackableObject report
//		(*this) << "[freeable=" << TrackableObject::GetFreeableSize()/1000000 << "M, unf.=" << TrackableObject::GetUnFreeableSize()/1000000 << "M] ";
	}
        #ifndef __BH_COMMUNITY__
        if (m_bResourceMonAtLock && m_bEnabled)
        {
		string rsstat = RMon.PrintStat();
		(*this) << '[' << rsstat.c_str() << "] ";
        }
        #endif
		
	return *this;
}

Channel& Channel::unlock()
{
	if(ConnectionInfoOnTLS.IsValid() && ConnectionInfoOnTLS->GetDisplayLock() > 0) {
		m_bEnabled = m_bEnabled_tmp;
	}
	
    channel_mutex.Unlock();
	//pthread_mutex_unlock(&m_synchr);
	return *this;
}

Channel& Channel::operator<<(short value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << value;
	return *this;
}

Channel& Channel::operator<<(int value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << value;
	return *this;
}

Channel& Channel::operator<<(float value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << value;
	return *this;
}

Channel& Channel::operator<<(double value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << value;
	return *this;
}

Channel& Channel::operator<<(long double value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << value;
	return *this;
}

Channel& Channel::operator<<(unsigned short value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << value;
	return *this;
}

Channel& Channel::operator<<(unsigned int value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << value;
	return *this;
}

Channel& Channel::operator<<(unsigned long value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << value;
	return *this;
}

Channel& Channel::operator<<( int long value )
{
	return operator << ( static_cast<int long long>( value ) );
}

Channel& Channel::operator<<( int long long value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) {
			if(value == NULL_VALUE_64)
				(*m_Outputs[out]) << "null";
			else if(value == PLUS_INF_64)
				(*m_Outputs[out]) << "+inf";
			else if(value == MINUS_INF_64)
				(*m_Outputs[out]) << "-inf";
			else
				(*m_Outputs[out]) << value;
		}
	return *this;
}

Channel& Channel::operator<<(unsigned long long value)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) {
			if(value == NULL_VALUE_64)
				(*m_Outputs[out]) << "null";
			else if(value == PLUS_INF_64)
				(*m_Outputs[out]) << "+inf";
			else if(value == MINUS_INF_64)
				(*m_Outputs[out]) << "-inf";
			else
				(*m_Outputs[out]) << value;
		}
	return *this;
}

Channel& Channel::operator<<(const char* buffer)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << buffer;
	return *this;
}

Channel& Channel::operator<<(const wchar_t* buffer)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << buffer;
	return *this;
}

Channel& Channel::operator<<(char c)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << c;
	return *this;
}
Channel& Channel::operator<<(const string& str)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << str;
	return *this;
}

Channel& Channel::operator<<(const exception& exc)
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) (*m_Outputs[out]) << exc.what();
	return *this;
}

Channel& Channel::flush()
{
	if (m_bEnabled)
		for (int out = 0; out < m_nNoOfOutputs; out++) m_Outputs[out]->flush();
	return *this;
}


