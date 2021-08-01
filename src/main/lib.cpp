#include <rdkafka4esl/Module.h>

#include <esl/module/Module.h>

extern "C" void esl__module__library__install(esl::module::Module* module) {
	if(module != nullptr) {
		rdkafka4esl::Module::install(*module);
	}
}
