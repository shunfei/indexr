#include "Rejecter.h"

using namespace std;

Rejecter::Rejecter(int packrowSize, string const& path, int64 abortOnCount, double abortOnThreshold)
	:	reject_file(path), abort_on_count(abortOnCount), abort_on_threshold(abortOnThreshold), writer(), packrow_size(packrowSize), rejected(0)
{
}

void Rejecter::ConsumeBadRow(char const* ptr, int64 size, int64 row_no, int error_code)
{
	bool do_throw(false);
	++rejected;
	if(!reject_file.empty()) {
		if(!writer.get()) {
			writer.reset(new IBFile());
			writer->OpenCreateNotExists(reject_file);
		}
		writer->WriteExact(ptr, (uint)size);
	}
	if(abort_on_threshold > 0) {
		if(row_no > packrow_size)
			do_throw = (static_cast<double>(rejected) / static_cast<double>(row_no)) >= abort_on_threshold;
	} else if(abort_on_count > 0) {
		do_throw = (rejected >= abort_on_count);
	} else if(abort_on_count == 0)
		do_throw = true;
	if(do_throw)
		throw FormatRCException(BHERROR_DATA_ERROR, row_no, error_code);
}
