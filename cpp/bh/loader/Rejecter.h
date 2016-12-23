#ifndef LOADER_REJECTER_H
#define LOADER_REJECTER_H 1

#include <string>
#include <boost/scoped_ptr.hpp>

#include "system/IBFile.h"

class Rejecter
{
	std::string reject_file;
	int64 abort_on_count;
	double abort_on_threshold;
	typedef boost::scoped_ptr<IBFile> writer_t;
	writer_t writer;
	int64 packrow_size;
	int64 rejected;
public:
	Rejecter(int, std::string const&, int64, double);
	void ConsumeBadRow(char const*, int64, int64, int);

	int64	GetNoRejectedRows() const { return rejected; }
	bool	ThresholdExceeded(int64 no_rows) const	{ return abort_on_threshold != 0 && (static_cast<double>(rejected) / static_cast<double>(no_rows)) >= abort_on_threshold; }
};

#endif /* #ifndef LOADER_REJECTER_H */

