// Copyright(c) 2006 to 2020 ZettaScale Technology and others
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

#include <org/eclipse/cyclonedds/core/EntityDelegate.hpp>
#include <org/eclipse/cyclonedds/sub/AnyDataReaderDelegate.hpp>
#include <org/eclipse/cyclonedds/core/ReportUtils.hpp>
#include <org/eclipse/cyclonedds/core/MiscUtils.hpp>
#include <org/eclipse/cyclonedds/core/ListenerDispatcher.hpp>
#include <org/eclipse/cyclonedds/core/ScopedLock.hpp>

#include <dds/core/cond/StatusCondition.hpp>
#include <dds/sub/AnyDataReaderListener.hpp>

#include "dds/dds.h"
#include "dds/ddsrt/sync.h"

#include <cassert>

org::eclipse::cyclonedds::core::EntityDelegate::EntityDelegate() :
  listener_mask(0),
  listener(NULL)
{
}

org::eclipse::cyclonedds::core::EntityDelegate::~EntityDelegate()
{
}

void org::eclipse::cyclonedds::core::EntityDelegate::enable()
{
  // TODO: no point in trying to emulate "enable" when core can't do it
}

::dds::core::status::StatusMask
org::eclipse::cyclonedds::core::EntityDelegate::status_changes() const
{
    ::dds::core::status::StatusMask mask;
    dds_return_t ret;
    uint32_t ddsc_mask;

    check();
    ret = dds_get_status_changes(ddsc_entity, &ddsc_mask);
    ISOCPP_DDSC_RESULT_CHECK_AND_THROW(ret, "Could not set internal listener.");
    mask = convertStatusMask(ddsc_mask);

    return mask;
}

::dds::core::InstanceHandle
org::eclipse::cyclonedds::core::EntityDelegate::instance_handle() const
{
    ::dds::core::InstanceHandle handle(::dds::core::null);
    dds_instance_handle_t ddsc_handle;
    dds_return_t ret;

    this->check();

    ret = dds_get_instance_handle(this->ddsc_entity, &ddsc_handle);
    ISOCPP_DDSC_RESULT_CHECK_AND_THROW(ret, "Failed get instance handle");
    handle = dds::core::InstanceHandle(ddsc_handle);

    return handle;
}

bool
org::eclipse::cyclonedds::core::EntityDelegate::contains_entity(
    const ::dds::core::InstanceHandle& handle)
{
    DDSCXX_UNUSED_ARG(handle);
    return false;
}

org::eclipse::cyclonedds::core::ObjectDelegate::ref_type
org::eclipse::cyclonedds::core::EntityDelegate::get_statusCondition()
{

    org::eclipse::cyclonedds::core::ScopedObjectLock scopedLock(*this);

    org::eclipse::cyclonedds::core::ObjectDelegate::ref_type mySCObj = this->myStatusCondition.lock();
    if (!mySCObj) {
        dds::core::cond::TStatusCondition<org::eclipse::cyclonedds::core::cond::StatusConditionDelegate> mySC(
                new org::eclipse::cyclonedds::core::cond::StatusConditionDelegate(this, this->ddsc_entity));
        mySC.delegate()->init(mySC.delegate());
        this->myStatusCondition = mySC.delegate()->get_weak_ref();
        mySCObj = mySC.delegate()->get_strong_ref();
    }

    return mySCObj;
}


void
org::eclipse::cyclonedds::core::EntityDelegate::close()
{
    org::eclipse::cyclonedds::core::ObjectDelegate::ref_type mySCObj = this->myStatusCondition.lock();
    if (mySCObj) {
        mySCObj->close();
    }
    org::eclipse::cyclonedds::core::DDScObjectDelegate::close();
}

void
org::eclipse::cyclonedds::core::EntityDelegate::retain()
{
    ISOCPP_THROW_EXCEPTION(ISOCPP_UNSUPPORTED_ERROR, "Function not currently supported");
}

void
org::eclipse::cyclonedds::core::EntityDelegate::listener_set(
                 void *_listener,
                 const dds::core::status::StatusMask& mask,
                 bool reset_on_invoke)
{
    dds_listener_t *callbacks;
    this->listener = _listener;
    this->listener_mask = mask;

    callbacks = dds_create_listener(nullptr);

    // Set topic callbacks
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::inconsistent_topic()))
    {
        dds_lset_inconsistent_topic_arg(callbacks, callback_on_inconsistent_topic, static_cast<void *>(this), reset_on_invoke);
    }

    // Set writer callbacks
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::offered_deadline_missed()))
    {
        dds_lset_offered_deadline_missed_arg(callbacks, callback_on_offered_deadline_missed, static_cast<void *>(this), reset_on_invoke);
    }
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::offered_incompatible_qos()))
    {
        dds_lset_offered_incompatible_qos_arg(callbacks, callback_on_offered_incompatible_qos, static_cast<void *>(this), reset_on_invoke);
    }
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::liveliness_lost()))
    {
        dds_lset_liveliness_lost_arg(callbacks, callback_on_liveliness_lost, static_cast<void *>(this), reset_on_invoke);
    }
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::publication_matched()))
    {
        dds_lset_publication_matched_arg(callbacks, callback_on_publication_matched, static_cast<void *>(this), reset_on_invoke);
    }

    // Set reader callbacks
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::requested_deadline_missed()))
    {
        dds_lset_requested_deadline_missed_arg(callbacks, callback_on_requested_deadline_missed, static_cast<void *>(this), reset_on_invoke);
    }
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::requested_incompatible_qos()))
    {
        dds_lset_requested_incompatible_qos_arg(callbacks, callback_on_requested_incompatible_qos, static_cast<void *>(this), reset_on_invoke);
    }
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::sample_rejected()))
    {
        dds_lset_sample_rejected_arg(callbacks, callback_on_sample_rejected, static_cast<void *>(this), reset_on_invoke);
    }
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::liveliness_changed()))
    {
        dds_lset_liveliness_changed_arg(callbacks, callback_on_liveliness_changed, static_cast<void *>(this), reset_on_invoke);
    }
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::data_available()))
    {
        dds_lset_data_available_arg(callbacks, callback_on_data_available, static_cast<void *>(this), reset_on_invoke);
    }
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::subscription_matched()))
    {
        dds_lset_subscription_matched_arg(callbacks, callback_on_subscription_matched, static_cast<void *>(this), reset_on_invoke);
    }
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::sample_lost()))
    {
        dds_lset_sample_lost_arg(callbacks, callback_on_sample_lost, static_cast<void *>(this), reset_on_invoke);
    }

    // Set subscriber callbacks
    if (STATUS_MASK_CONTAINS(mask, dds::core::status::StatusMask::data_on_readers()))
    {
        dds_lset_data_on_readers_arg(callbacks, callback_on_data_readers, static_cast<void *>(this), reset_on_invoke);
    }

    // If entity enabled: set listener on ddsc entity
    {
        dds_return_t ret;
        ret = dds_set_listener(this->ddsc_entity, callbacks);
        ISOCPP_DDSC_RESULT_CHECK_AND_THROW(ret, "Setting listener failed.");
    }
  
    dds_delete_listener(callbacks);
}

void * org::eclipse::cyclonedds::core::EntityDelegate::listener_get () const
{
  return this->listener;
}

const dds::core::status::StatusMask
org::eclipse::cyclonedds::core::EntityDelegate::get_listener_mask () const
{
  return this->listener_mask;
}

// These are to satisfy the linker, they should never be called
void org::eclipse::cyclonedds::core::EntityDelegate::on_inconsistent_topic
  (dds_entity_t, org::eclipse::cyclonedds::core::InconsistentTopicStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_offered_deadline_missed
  (dds_entity_t, org::eclipse::cyclonedds::core::OfferedDeadlineMissedStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_offered_incompatible_qos
  (dds_entity_t, org::eclipse::cyclonedds::core::OfferedIncompatibleQosStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_liveliness_lost
  (dds_entity_t, org::eclipse::cyclonedds::core::LivelinessLostStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_publication_matched
  (dds_entity_t, org::eclipse::cyclonedds::core::PublicationMatchedStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_requested_deadline_missed
  (dds_entity_t, org::eclipse::cyclonedds::core::RequestedDeadlineMissedStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_requested_incompatible_qos
  (dds_entity_t, org::eclipse::cyclonedds::core::RequestedIncompatibleQosStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_sample_rejected
  (dds_entity_t, org::eclipse::cyclonedds::core::SampleRejectedStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_liveliness_changed
  (dds_entity_t, org::eclipse::cyclonedds::core::LivelinessChangedStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_data_available (dds_entity_t)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_subscription_matched
  (dds_entity_t, org::eclipse::cyclonedds::core::SubscriptionMatchedStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_sample_lost
  (dds_entity_t, org::eclipse::cyclonedds::core::SampleLostStatusDelegate &)
{
  assert (false);
}

void org::eclipse::cyclonedds::core::EntityDelegate::on_data_readers(dds_entity_t)
{
  assert (false);
}
