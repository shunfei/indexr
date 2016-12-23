#ifndef DESTRUCTIONDEBUGGER_H_INCLUDED
#define DESTRUCTIONDEBUGGER_H_INCLUDED 1

#include <map>
#include <boost/version.hpp>

#ifndef NDEBUG
#define DEBUG_STATEMENT(statement) statement
#if ( BOOST_VERSION / 100 % 1000 ) >= 35 && !defined(__sun__)

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

template<typename base_t>
class DestructionDebugger
{
public:
	typedef std::map<boost::thread::id, int long> counter_t;
private:
	static boost::mutex mutex;
	static counter_t counter;
public:
	DestructionDebugger(void) {
		boost::mutex::scoped_lock l(mutex);
		++ counter[boost::this_thread::get_id()];
	}
	DestructionDebugger(DestructionDebugger const&) {
		boost::mutex::scoped_lock l(mutex);
		++ counter[boost::this_thread::get_id()];
	}
	virtual ~DestructionDebugger(void) {
		boost::mutex::scoped_lock l(mutex);
		-- counter[boost::this_thread::get_id()];
	}
	static int long get_instance_count(void) {
		boost::mutex::scoped_lock l(mutex);
		return counter[boost::this_thread::get_id()];
	}
};

template<typename base_t>
typename DestructionDebugger<base_t>::counter_t DestructionDebugger<base_t>::counter;
template<typename base_t>
boost::mutex DestructionDebugger<base_t>::mutex;
#else

template<typename base_t>
class DestructionDebugger
{
public:
	DestructionDebugger(void) {
	}
	DestructionDebugger(DestructionDebugger const&) {
	}
	virtual ~DestructionDebugger(void) {
	}
	static int long get_instance_count(void) {
		return 0;
	}
};

#endif
#else
#define DEBUG_STATEMENT( statement ) /**/

#endif

#endif /* not DESTRUCTIONDEBUGGER_H_INCLUDED */
