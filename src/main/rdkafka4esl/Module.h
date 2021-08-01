#ifndef RDKAFKA4ESL_MODULE_H_
#define RDKAFKA4ESL_MODULE_H_

#include <esl/module/Module.h>

namespace rdkafka4esl {

struct Module final {
	Module() = delete;
	static void install(esl::module::Module& module);
};

} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MODULE_H_ */
