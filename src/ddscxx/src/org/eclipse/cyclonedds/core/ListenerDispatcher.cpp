// Copyright(c) 2006 to 2021 ZettaScale Technology and others
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License v. 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Eclipse Distribution License
// v. 1.0 which is available at
// http://www.eclipse.org/org/documents/edl-v10.php.
//
// SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

/**
 * @file
 */

#include <org/eclipse/cyclonedds/core/ScopedLock.hpp>
#include <org/eclipse/cyclonedds/core/ListenerDispatcher.hpp>
#include <dds/topic/AnyTopic.hpp>
#include <org/eclipse/cyclonedds/topic/AnyTopicDelegate.hpp>
#include <org/eclipse/cyclonedds/pub/AnyDataWriterDelegate.hpp>

#include "dds/dds.h"


#ifdef _WIN32_DLL_
  #define DDS_FN_EXPORT __declspec (dllexport)
#else
  #define DDS_FN_EXPORT
#endif

extern "C"
{
  // Topic callback
  DDS_FN_EXPORT void callback_on_inconsistent_topic
    (dds_entity_t topic, dds_inconsistent_topic_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::InconsistentTopicStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_inconsistent_topic(topic, sd);
  }

  // Writer callbacks
  DDS_FN_EXPORT void callback_on_offered_deadline_missed
    (dds_entity_t writer, dds_offered_deadline_missed_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::OfferedDeadlineMissedStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_offered_deadline_missed(writer, sd);
  }

  DDS_FN_EXPORT void callback_on_offered_incompatible_qos
    (dds_entity_t writer, dds_offered_incompatible_qos_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::OfferedIncompatibleQosStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_offered_incompatible_qos(writer, sd);
  }

  DDS_FN_EXPORT void callback_on_liveliness_lost
    (dds_entity_t writer, dds_liveliness_lost_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::LivelinessLostStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_liveliness_lost(writer, sd);
  }

  DDS_FN_EXPORT void callback_on_publication_matched
    (dds_entity_t writer, dds_publication_matched_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::PublicationMatchedStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_publication_matched(writer, sd);
  }

  // Reader callbacks
  DDS_FN_EXPORT void callback_on_requested_deadline_missed
    (dds_entity_t reader, dds_requested_deadline_missed_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::RequestedDeadlineMissedStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_requested_deadline_missed(reader, sd);
  }

  DDS_FN_EXPORT void callback_on_requested_incompatible_qos
    (dds_entity_t reader, dds_requested_incompatible_qos_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::RequestedIncompatibleQosStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_requested_incompatible_qos(reader, sd);
  }

  DDS_FN_EXPORT void callback_on_sample_rejected
    (dds_entity_t reader, dds_sample_rejected_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::SampleRejectedStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_sample_rejected(reader, sd);
  }

  DDS_FN_EXPORT void callback_on_liveliness_changed
    (dds_entity_t reader, dds_liveliness_changed_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::LivelinessChangedStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_liveliness_changed(reader, sd);
  }

  DDS_FN_EXPORT void callback_on_data_available (dds_entity_t reader, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    cpp_ref->on_data_available(reader);
  }

  DDS_FN_EXPORT void callback_on_subscription_matched
    (dds_entity_t reader, dds_subscription_matched_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::SubscriptionMatchedStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_subscription_matched(reader, sd);
  }

  DDS_FN_EXPORT void callback_on_sample_lost
    (dds_entity_t reader, dds_sample_lost_status_t status, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    org::eclipse::cyclonedds::core::SampleLostStatusDelegate sd;
    sd.ddsc_status(&status);
    cpp_ref->on_sample_lost(reader, sd);
  }

  // Subscriber callback
  DDS_FN_EXPORT void callback_on_data_readers (dds_entity_t subscriber, void* arg)
  {
    auto cpp_ref = reinterpret_cast<::org::eclipse::cyclonedds::core::EntityDelegate *>(arg);
    cpp_ref->on_data_readers(subscriber);
  }
}
