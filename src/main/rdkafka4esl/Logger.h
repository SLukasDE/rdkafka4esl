#ifndef RDKAFKA4ESL_LOGGER_H_
#define RDKAFKA4ESL_LOGGER_H_

#include <esl/logging/Logger.h>
#include <esl/logging/Level.h>

namespace rdkafka4esl {
using Logger = esl::logging::Logger<esl::logging::Level::TRACE>;
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_LOGGER_H_ */
