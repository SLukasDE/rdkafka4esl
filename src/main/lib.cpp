#include <esl/module/Library.h>
#include <rdkafka4esl/Module.h>

extern "C" esl::module::Module* esl__module__library__getModule(const std::string& moduleName) {
	return &rdkafka4esl::getModule();
}
