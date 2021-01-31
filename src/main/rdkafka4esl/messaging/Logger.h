#ifndef RDKAFKA4ESL_MESSAGING_LOGGER_H_
#define RDKAFKA4ESL_MESSAGING_LOGGER_H_

#include <esl/logging/Logger.h>
#include <esl/logging/Level.h>

namespace rdkafka4esl {
namespace messaging {

using Logger = esl::logging::Logger<esl::logging::Level::TRACE>;

} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_LOGGER_H_ */
