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

#include "SharedMemOut.h"

using namespace std;

SharedMemOut::SharedMemOut(char* msg_buf, unsigned buf_size, const char* event_name, const char *shpath, int &status)
:m_msg_buf(msg_buf),m_buf_size(buf_size)
{
	m_out = new ostringstream;
	status= 0;
	char* eventname_for_loader = new char[strlen(event_name)+2];
	strcpy(eventname_for_loader, event_name);
	strcat(eventname_for_loader, "a");
#ifdef _MSC_VER
	m_event_for_server = OpenEventA(EVENT_MODIFY_STATE, FALSE, event_name);
	m_event_for_loader = OpenEventA(EVENT_ALL_ACCESS, FALSE, eventname_for_loader);
#else
//	printf("/Debug Message, sharedout = %s\n", shpath);
	if ((sem_ipc_key= ftok(shpath, 1)) == (key_t) -1)
	{
//		printf("/Debug Message, shared out %s\n", strerror(errno));
		delete [] eventname_for_loader;
		return;
	}
//    printf("/Debug Message, sharedout sem_ipc_key=%d\n", sem_ipc_key);
    if ((sem_ipc= semget(sem_ipc_key, 2, 0)) < 0)
    {
//		printf("/Debug Message, sharedout failed sem_id=%d\n",sem_ipc);
		delete [] eventname_for_loader;
		return;
    }
//	printf("/Debug Message, sharedout sem_sid=%d\n",sem_ipc);
#endif
	delete [] eventname_for_loader;
    status= 1;
	return;
};

SharedMemOut::~SharedMemOut()
{
#ifdef _MSC_VER
	CloseHandle(m_event_for_loader);
	CloseHandle(m_event_for_server);
#endif
	delete m_out;
};

ChannelOut& SharedMemOut::flush()
{
	//To do: flush all to shared memory
	size_t len = m_out->str().length();
	if (len>0 && m_out->str().at(len-1)=='\n') len--;
	if (len >= m_buf_size) len = m_buf_size-1;
	unsigned pos;
	for (pos=0; pos<len; pos++)
	  m_msg_buf[pos]= m_out->str().at(pos);
	m_msg_buf[pos]= 0;
#ifdef _MSC_VER
	SetEvent(m_event_for_server);
	delete m_out;
	m_out = new ostringstream;
	WaitForSingleObject(m_event_for_loader, INFINITE);
#else
  int a;

  struct sembuf op_wait={1,-1,SEM_UNDO|IPC_WAIT};
  struct sembuf op_signal={0,1,SEM_UNDO|IPC_WAIT};

  if (semop(sem_ipc,&op_signal,1))
  {
	a=1;
  }

  delete m_out;
  m_out = new ostringstream;

  if (semop(sem_ipc,&op_wait,1))
  {
	a=2;
  }
#endif
	return *this;
};
