# Filestore Backend: Inventory of Database Behavior

Everything below is enforced by Postgres today (`migrate/schema.sql`). The
file backend has no database to do it, so each item needs Go code that does
the same thing and a test that proves it (F2, F7, F8), or a note that it is
dropped with the feature it serves (F24). Phase 3 fills in the **Go** line
for each table as its store is ported.

Totals: 61 tables, 59 triggers, 82 foreign keys, 31 checks, 165 unique
constraints and indexes, and 67 functions (52 of them run by triggers).

Generated from `migrate/schema.sql` at `2c33d971a`.

## Functions not run by a trigger

These are called from SQL or Go. Each call site moves with the code that uses it.

- `aquire_user_contact_method_lock`
- `cm_type_val_to_dest`
- `escalate_alerts`
- `fn_disable_inserts`
- `fn_insert_user_last_alert_log`
- `fn_job_rotation`
- `fn_update_user_last_alert_log`
- `fn_util_river_job`
- `move_escalation_policy_step`
- `move_rotation_position`
- `nc_type_val_to_dest`
- `release_user_contact_method_lock`
- `remove_rotation_participant`
- `river_job_state_in_bitmask`
- `update_notification_cycles`

## Tables

### alert_data

Foreign keys:

- alert_data_alert_id_fkey: `(alert_id) REFERENCES alerts(id) ON DELETE CASCADE`

Unique:

- alert_data_id_key (index): `(id)`
- alert_data_id_key: `(id)`
- alert_data_pkey (index): `(alert_id)`

**Go:** not ported yet.

### alert_feedback

Foreign keys:

- alert_feedback_alert_id_fkey: `(alert_id) REFERENCES alerts(id) ON DELETE CASCADE`

Unique:

- alert_feedback_id_key (index): `(id)`
- alert_feedback_id_key: `(id)`
- alert_feedback_pkey (index): `(alert_id)`

**Go:** not ported yet.

### alert_logs

Unique:

- alert_logs_pkey (index): `(id)`

**Go:** not ported yet.

### alert_metrics

Foreign keys:

- alert_metrics_alert_id_fkey: `(alert_id) REFERENCES alerts(id) ON DELETE CASCADE`

Unique:

- alert_metrics_id_key (index): `(id)`
- alert_metrics_id_key: `(id)`
- alert_metrics_pkey (index): `(alert_id)`

**Go:** not ported yet.

### alert_status_subscriptions

Foreign keys:

- alert_status_subscriptions_alert_id_fkey: `(alert_id) REFERENCES alerts(id) ON DELETE CASCADE`
- alert_status_subscriptions_channel_id_fkey: `(channel_id) REFERENCES notification_channels(id) ON DELETE CASCADE`
- alert_status_subscriptions_contact_method_id_fkey: `(contact_method_id) REFERENCES user_contact_methods(id) ON DELETE CASCADE`

Checks:

- alert_status_subscriptions_check: `((channel_id IS NULL) <> (contact_method_id IS NULL))`

Unique:

- alert_status_subscriptions_channel_id_contact_method_id_ale_key (index): `(channel_id, contact_method_id, alert_id)`
- alert_status_subscriptions_channel_id_contact_method_id_ale_key: `(channel_id, contact_method_id, alert_id)`
- alert_status_subscriptions_pkey (index): `(id)`

**Go:** not ported yet.

### alerts

Triggers:

- trg_10_clear_ep_state_on_alert_close: AFTER update when `((old.status <> new.status) AND (new.status = 'closed'::enum_alert_status))`, runs `fn_clear_ep_state_on_alert_close`
- trg_10_insert_ep_state_on_alert_insert: AFTER insert when `(new.status <> 'closed'::enum_alert_status)`, runs `fn_insert_ep_state_on_alert_insert`
- trg_20_clear_next_esc_on_alert_ack: AFTER update when `((new.status <> old.status) AND (old.status = 'active'::enum_alert_status))`, runs `fn_clear_next_esc_on_alert_ack`
- trg_clear_dedup_on_close: BEFORE update when `((new.status <> old.status) AND (new.status = 'closed'::enum_alert_status))`, runs `fn_clear_dedup_on_close`
- trg_enforce_alert_limit: AFTER insert, runs `fn_enforce_alert_limit`
- trg_prevent_reopen: BEFORE update of status, runs `fn_prevent_reopen`
- trg_track_alert_status_update: AFTER update when `(new.status IS DISTINCT FROM old.status)`, runs `fn_track_alert_status`

Foreign keys:

- alerts_services_id_fkey: `(service_id) REFERENCES services(id) ON DELETE CASCADE`

Checks:

- dedup_key_only_for_open_alerts: `((status = 'closed'::enum_alert_status) = (dedup_key IS NULL))`

Unique:

- alerts_pkey (index): `(id)`
- idx_no_alert_duplicates (index): `(service_id, dedup_key)`

**Go:** not ported yet.

### auth_basic_users

Triggers:

- trg_insert_basic_user: AFTER insert, runs `fn_insert_basic_user`

Foreign keys:

- auth_basic_users_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- auth_basic_users_pkey (index): `(user_id)`
- auth_basic_users_uniq_id (index): `(id)`
- auth_basic_users_uniq_id: `(id)`
- auth_basic_users_username_key (index): `(username)`
- auth_basic_users_username_key: `(username)`

**Go:** not ported yet.

### auth_link_requests

Unique:

- auth_link_requests_pkey (index): `(id)`

**Go:** not ported yet.

### auth_nonce

Unique:

- auth_nonce_pkey (index): `(id)`

**Go:** ported, `auth/nonce/delta.go`. The primary key is the table key;
`created_at DEFAULT now()` is the transaction's time. Cleanup after a week, as
on Postgres.

### auth_subjects

Foreign keys:

- auth_subjects_cm_id_fkey: `(cm_id) REFERENCES user_contact_methods(id) ON DELETE CASCADE`
- auth_subjects_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- auth_subjects_pkey (index): `(provider_id, subject_id)`
- auth_subjects_uniq_id (index): `(id)`
- auth_subjects_uniq_id: `(id)`

**Go:** not ported yet.

### auth_user_sessions

Foreign keys:

- auth_user_sessions_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- auth_user_sessions_pkey (index): `(id)`

**Go:** not ported yet.

### config

Triggers:

- trg_config_update: AFTER insert, runs `fn_notify_config_refresh`

Unique:

- config_pkey (index): `(id)`

**Go:** not ported yet.

### config_limits

Unique:

- config_limits_pkey (index): `(id)`

**Go:** not ported yet.

### engine_processing_versions

Unique:

- engine_processing_versions_pkey (index): `(type_id)`

**Go:** not ported yet.

### entity_updates

Unique:

- entity_updates_pkey (index): `(id)`

**Go:** not ported yet.

### ep_step_on_call_users

Foreign keys:

- ep_step_on_call_users_ep_step_id_fkey: `(ep_step_id) REFERENCES escalation_policy_steps(id) ON DELETE CASCADE`
- ep_step_on_call_users_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- ep_step_on_call_users_uniq_id (index): `(id)`
- ep_step_on_call_users_uniq_id: `(id)`
- idx_ep_step_on_call (index): `(user_id, ep_step_id) WHERE (end_time IS NULL)`

**Go:** not ported yet.

### escalation_policies

Unique:

- escalation_policies_name (index): `(lower(name))`
- escalation_policies_name_key (index): `(name)`
- escalation_policies_name_key: `(name)`
- escalation_policies_pkey (index): `(id)`

**Go:** not ported yet.

### escalation_policy_actions

Triggers:

- trg_enforce_ep_step_action_limit: AFTER insert, runs `fn_enforce_ep_step_action_limit`

Foreign keys:

- escalation_policy_actions_channel_id_fkey: `(channel_id) REFERENCES notification_channels(id) ON DELETE CASCADE`
- escalation_policy_actions_escalation_policy_step_id_fkey: `(escalation_policy_step_id) REFERENCES escalation_policy_steps(id) ON DELETE CASCADE`
- escalation_policy_actions_rotation_id_fkey: `(rotation_id) REFERENCES rotations(id) ON DELETE CASCADE`
- escalation_policy_actions_schedule_id_fkey1: `(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE`
- escalation_policy_actions_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Checks:

- epa_there_can_only_be_one: `((`

Unique:

- epa_no_duplicate_channels (index): `(escalation_policy_step_id, channel_id)`
- epa_no_duplicate_channels: `(escalation_policy_step_id, channel_id)`
- epa_no_duplicate_rotations (index): `(escalation_policy_step_id, rotation_id)`
- epa_no_duplicate_rotations: `(escalation_policy_step_id, rotation_id)`
- epa_no_duplicate_schedules (index): `(escalation_policy_step_id, schedule_id)`
- epa_no_duplicate_schedules: `(escalation_policy_step_id, schedule_id)`
- epa_no_duplicate_users (index): `(escalation_policy_step_id, user_id)`
- epa_no_duplicate_users: `(escalation_policy_step_id, user_id)`
- escalation_policy_actions_pkey (index): `(id)`

**Go:** not ported yet.

### escalation_policy_state

Triggers:

- trg_10_set_ep_state_svc_id_on_insert: BEFORE insert when `(new.service_id IS NULL)`, runs `fn_set_ep_state_svc_id_on_insert`
- trg_20_lock_svc_on_force_escalation: BEFORE update when `((new.force_escalation <> old.force_escalation) AND new.force_escalation)`, runs `fn_lock_svc_on_force_escalation`
- trg_30_trig_alert_on_force_escalation: AFTER update when `((new.force_escalation <> old.force_escalation) AND new.force_escalation)`, runs `fn_trig_alert_on_force_escalation`

Foreign keys:

- escalation_policy_state_alert_id_fkey: `(alert_id) REFERENCES alerts(id) ON DELETE CASCADE`
- escalation_policy_state_escalation_policy_id_fkey: `(escalation_policy_id) REFERENCES escalation_policies(id) ON DELETE CASCADE`
- escalation_policy_state_escalation_policy_step_id_fkey: `(escalation_policy_step_id) REFERENCES escalation_policy_steps(id) ON DELETE SET NULL`
- escalation_policy_state_service_id_fkey: `(service_id) REFERENCES services(id) ON DELETE CASCADE`
- svc_ep_fkey: `(service_id, escalation_policy_id) REFERENCES services(id, escalation_policy_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE`

Unique:

- escalation_policy_state_pkey (index): `(alert_id)`
- escalation_policy_state_uniq_id (index): `(id)`
- escalation_policy_state_uniq_id: `(id)`

**Go:** not ported yet.

### escalation_policy_steps

Triggers:

- trg_10_decr_ep_step_count_on_del: BEFORE delete, runs `fn_decr_ep_step_count_on_del`
- trg_10_incr_ep_step_count_on_add: BEFORE insert, runs `fn_incr_ep_step_count_on_add`
- trg_10_insert_ep_state_on_step_insert: AFTER insert when `(new.step_number = 0)`, runs `fn_insert_ep_state_on_step_insert`
- trg_decr_ep_step_number_on_delete: AFTER delete, runs `fn_decr_ep_step_number_on_delete`
- trg_enforce_ep_step_limit: AFTER insert, runs `fn_enforce_ep_step_limit`
- trg_ep_step_number_no_gaps: AFTER update, runs `fn_enforce_ep_step_number_no_gaps`
- trg_inc_ep_step_number_on_insert: BEFORE insert, runs `fn_inc_ep_step_number_on_insert`

Foreign keys:

- escalation_policy_steps_escalation_policy_id_fkey: `(escalation_policy_id) REFERENCES escalation_policies(id) ON DELETE CASCADE`

Unique:

- escalation_policy_steps_escalation_policy_id_step_number_key (index): `(escalation_policy_id, step_number)`
- escalation_policy_steps_escalation_policy_id_step_number_key: `(escalation_policy_id, step_number) DEFERRABLE INITIALLY DEFERRED`
- escalation_policy_steps_pkey (index): `(id)`

**Go:** not ported yet.

### gorp_migrations

Unique:

- gorp_migrations_pkey (index): `(id)`

**Go:** not ported yet.

### gql_api_key_usage

Foreign keys:

- gql_api_key_usage_api_key_id_fkey: `(api_key_id) REFERENCES gql_api_keys(id) ON DELETE CASCADE`

Unique:

- gql_api_key_usage_api_key_id_key (index): `(api_key_id)`
- gql_api_key_usage_api_key_id_key: `(api_key_id)`
- gql_api_key_usage_pkey (index): `(id)`

**Go:** not ported yet.

### gql_api_keys

Foreign keys:

- gql_api_keys_created_by_fkey: `(created_by) REFERENCES users(id) ON DELETE SET NULL`
- gql_api_keys_deleted_by_fkey: `(deleted_by) REFERENCES users(id) ON DELETE SET NULL`
- gql_api_keys_updated_by_fkey: `(updated_by) REFERENCES users(id) ON DELETE SET NULL`

Unique:

- gql_api_keys_name_key (index): `(name) WHERE (deleted_at IS NULL)`
- gql_api_keys_pkey (index): `(id)`

**Go:** not ported yet.

### heartbeat_monitors

Triggers:

- trg_enforce_heartbeat_monitor_limit: AFTER insert, runs `fn_enforce_heartbeat_limit`

Foreign keys:

- heartbeat_monitors_service_id_fkey: `(service_id) REFERENCES services(id) ON DELETE CASCADE`

Unique:

- heartbeat_monitor_name_service_id (index): `(lower(name), service_id)`
- heartbeat_monitors_pkey (index): `(id)`

**Go:** not ported yet.

### integration_keys

Triggers:

- trg_enforce_integration_key_limit: AFTER insert, runs `fn_enforce_integration_key_limit`

Foreign keys:

- integration_keys_services_id_fkey: `(service_id) REFERENCES services(id) ON DELETE CASCADE`

Unique:

- idx_int_key_name_svc_ext (index): `(lower(name), service_id, COALESCE(external_system_name, ''::text))`
- integration_keys_pkey (index): `(id)`

**Go:** not ported yet.

### keyring

Unique:

- keyring_pkey (index): `(id)`

**Go:** not ported yet.

### labels

Foreign keys:

- labels_tgt_ep_id_fkey: `(tgt_ep_id) REFERENCES escalation_policies(id) ON DELETE CASCADE`
- labels_tgt_rotation_id_fkey: `(tgt_rotation_id) REFERENCES rotations(id) ON DELETE CASCADE`
- labels_tgt_schedule_id_fkey: `(tgt_schedule_id) REFERENCES schedules(id) ON DELETE CASCADE`
- labels_tgt_service_id_fkey: `(tgt_service_id) REFERENCES services(id) ON DELETE CASCADE`

Unique:

- idx_labels_resource_id (index): `(tgt_service_id, tgt_schedule_id, tgt_rotation_id, tgt_ep_id, key)`
- labels_pkey (index): `(id)`

**Go:** not ported yet.

### message_status_history

Foreign keys:

- message_status_history_message_id_fkey: `(message_id) REFERENCES outgoing_messages(id) ON DELETE CASCADE`

Unique:

- message_status_history_pkey (index): `(id)`

**Go:** not ported yet.

### notification_channel_duplicates

Unique:

- notification_channel_duplicates_id_key (index): `(id)`
- notification_channel_duplicates_id_key: `(id)`
- notification_channel_duplicates_pkey (index): `(old_id)`

**Go:** not ported yet.

### notification_channels

Triggers:

- trg_10_nc_compat_set_type_val_on_insert: BEFORE insert when `(new.dest IS NOT NULL)`, runs `fn_nc_compat_set_type_val_on_insert`
- trg_10_nc_compat_set_type_val_on_update: BEFORE update when `(new.dest <> old.dest)`, runs `fn_nc_compat_set_type_val_on_insert`
- trg_10_nc_set_dest_on_insert: BEFORE insert when `(new.dest IS NULL)`, runs `fn_nc_set_dest_on_insert`
- trg_10_nc_set_dest_on_update: AFTER update when `((new.dest = old.dest) AND ((new.type <> 'DEST'::enum_notif_channel_type) AND ((new.value <> old.value) OR (new.type <> old.type))))`, runs `fn_nc_set_dest_on_insert`

Unique:

- nc_unique_type_value (index): `(type, value)`
- nc_unique_type_value: `(type, value)`
- notification_channels_dest_key (index): `(dest)`
- notification_channels_dest_key: `(dest)`
- notification_channels_pkey (index): `(id)`

**Go:** not ported yet.

### notification_policy_cycles

Foreign keys:

- notification_policy_cycles_alert_id_fkey: `(alert_id) REFERENCES alerts(id) ON DELETE CASCADE`
- notification_policy_cycles_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- notification_policy_cycles_pkey (index): `(id)`

**Go:** not ported yet.

### outgoing_messages

Triggers:

- trg_insert_message_status_history: AFTER insert, runs `fn_insert_message_status_history`
- trg_update_message_status_history: AFTER update of last_status, runs `fn_insert_message_status_history`

Foreign keys:

- outgoing_messages_alert_id_fkey: `(alert_id) REFERENCES alerts(id) ON DELETE CASCADE`
- outgoing_messages_alert_log_id_fkey: `(alert_log_id) REFERENCES alert_logs(id) ON DELETE CASCADE`
- outgoing_messages_channel_id_fkey: `(channel_id) REFERENCES notification_channels(id) ON DELETE CASCADE`
- outgoing_messages_contact_method_id_fkey: `(contact_method_id) REFERENCES user_contact_methods(id) ON DELETE CASCADE`
- outgoing_messages_cycle_id_fkey: `(cycle_id) REFERENCES notification_policy_cycles(id) ON DELETE CASCADE`
- outgoing_messages_escalation_policy_id_fkey: `(escalation_policy_id) REFERENCES escalation_policies(id) ON DELETE CASCADE`
- outgoing_messages_schedule_id_fkey: `(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE`
- outgoing_messages_service_id_fkey: `(service_id) REFERENCES services(id) ON DELETE CASCADE`
- outgoing_messages_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`
- outgoing_messages_user_verification_code_id_fkey: `(user_verification_code_id) REFERENCES user_verification_codes(id) ON DELETE CASCADE`

Checks:

- om_alert_svc_ep_ids: `(message_type <> 'alert_notification'::enum_outgoing_messages_type OR alert_id IS NOT NULL AND service_id IS NOT NULL AND escalation_policy_id IS NOT NULL)`
- om_no_status_bundles: `(message_type <> 'alert_status_update_bundle'::enum_outgoing_messages_type OR last_status <> 'pending'::enum_outgoing_messages_status)`
- om_pending_no_fired_no_sent: `(last_status <> 'pending'::enum_outgoing_messages_status OR fired_at IS NULL AND sent_at IS NULL)`
- om_processed_no_fired_sent: `((last_status = ANY (ARRAY['pending'::enum_outgoing_messages_status, 'sending'::enum_outgoing_messages_status, 'failed'::enum_outgoing_messages_status, 'bundled'::enum_outgoing_messages_status])) OR fired_at IS NULL AND sent_at IS NOT NULL)`
- om_sending_deadline_reqd: `(last_status <> 'sending'::enum_outgoing_messages_status OR sending_deadline IS NOT NULL)`
- om_sending_fired_no_sent: `(last_status <> 'sending'::enum_outgoing_messages_status OR fired_at IS NOT NULL AND sent_at IS NULL)`
- om_status_alert_ids: `(message_type <> 'alert_status_update_bundle'::enum_outgoing_messages_type OR status_alert_ids IS NOT NULL)`
- om_status_update_log_id: `(message_type <> 'alert_status_update'::enum_outgoing_messages_type OR alert_log_id IS NOT NULL)`
- om_user_cm_or_channel: `(user_id IS NOT NULL AND contact_method_id IS NOT NULL AND channel_id IS NULL OR channel_id IS NOT NULL AND contact_method_id IS NULL AND user_id IS NULL)`
- verify_needs_id: `(message_type <> 'verification_message'::enum_outgoing_messages_type OR user_verification_code_id IS NOT NULL)`

Unique:

- idx_outgoing_messages_provider_msg_id (index): `(provider_msg_id)`
- outgoing_messages_pkey (index): `(id)`

**Go:** not ported yet.

### pending_signals

Triggers:

- trg_enforce_signals_per_dest_per_service_limit: AFTER insert, runs `fn_enforce_signals_per_dest_per_service_limit`
- trg_enforce_signals_per_service_limit: AFTER insert, runs `fn_enforce_signals_per_service_limit`
- trg_pending_signals_after_insert: AFTER insert, runs `fn_job_signal`

Foreign keys:

- pending_signals_dest_id_fkey: `(dest_id) REFERENCES notification_channels(id) ON DELETE CASCADE`
- pending_signals_message_id_fkey: `(message_id) REFERENCES outgoing_messages(id) ON DELETE CASCADE`
- pending_signals_service_id_fkey: `(service_id) REFERENCES services(id) ON DELETE CASCADE`

Unique:

- pending_signals_message_id_key (index): `(message_id)`
- pending_signals_message_id_key: `(message_id)`
- pending_signals_pkey (index): `(id)`

**Go:** not ported yet.

### region_ids

Unique:

- region_ids_id_key (index): `(id)`
- region_ids_id_key: `(id)`
- region_ids_pkey (index): `(name)`

**Go:** not ported yet.

### river_client

Checks:

- name_length: `(char_length(id) > 0 AND char_length(id) < 128)`

Unique:

- river_client_pkey (index): `(id)`

**Go:** not ported yet.

### river_client_queue

Foreign keys:

- river_client_queue_river_client_id_fkey: `(river_client_id) REFERENCES river_client(id) ON DELETE CASCADE`

Checks:

- name_length: `(char_length(name) > 0 AND char_length(name) < 128)`
- num_jobs_completed_zero_or_positive: `(num_jobs_completed >= 0)`
- num_jobs_running_zero_or_positive: `(num_jobs_running >= 0)`

Unique:

- river_client_queue_id_key (index): `(id)`
- river_client_queue_id_key: `(id)`
- river_client_queue_pkey (index): `(river_client_id, name)`

**Go:** not ported yet.

### river_job

Checks:

- finalized_or_finalized_at_null: `(finalized_at IS NULL AND (state <> ALL (ARRAY['cancelled'::river_job_state, 'completed'::river_job_state, 'discarded'::river_job_state])) OR finalized_at IS NOT NULL AND (state = ANY (ARRAY['cancelled'::river_job_state, 'completed'::river_job_state, 'discarded'::river_job_state])))`
- kind_length: `(char_length(kind) > 0 AND char_length(kind) < 128)`
- max_attempts_is_positive: `(max_attempts > 0)`
- priority_in_range: `(priority >= 1 AND priority <= 4)`
- queue_length: `(char_length(queue) > 0 AND char_length(queue) < 128)`

Unique:

- river_job_pkey (index): `(id)`
- river_job_unique_idx (index): `(unique_key) WHERE ((unique_key IS NOT NULL) AND (unique_states IS NOT NULL) AND river_job_state_in_bitmask(unique_states, state))`

**Go:** not ported yet.

### river_leader

Checks:

- leader_id_length: `(char_length(leader_id) > 0 AND char_length(leader_id) < 128)`
- name_length: `(name = 'default'::text)`

Unique:

- river_leader_id_key (index): `(id)`
- river_leader_id_key: `(id)`
- river_leader_pkey (index): `(name)`

**Go:** not ported yet.

### river_queue

Unique:

- river_queue_id_key (index): `(id)`
- river_queue_id_key: `(id)`
- river_queue_pkey (index): `(name)`

**Go:** not ported yet.

### rotation_participants

Triggers:

- trg_10_decr_part_count_on_del: BEFORE delete, runs `fn_decr_part_count_on_del`
- trg_20_decr_rot_part_position_on_delete: AFTER delete, runs `fn_decr_rot_part_position_on_delete`
- trg_30_advance_or_end_rot_on_part_del: BEFORE delete, runs `fn_advance_or_end_rot_on_part_del`
- trg_enforce_rot_part_position_no_gaps: AFTER update, runs `fn_enforce_rot_part_position_no_gaps`
- trg_enforce_rotation_participant_limit: AFTER insert, runs `fn_enforce_rotation_participant_limit`
- trg_inc_rot_part_position_on_insert: BEFORE insert, runs `fn_inc_rot_part_position_on_insert`
- trg_incr_part_count_on_add: BEFORE insert, runs `fn_incr_part_count_on_add`
- trg_set_rot_state_pos_on_part_reorder: BEFORE update when `(new."position" <> old."position")`, runs `fn_set_rot_state_pos_on_part_reorder`
- trg_start_rotation_on_first_part_add: AFTER insert, runs `fn_start_rotation_on_first_part_add`
- trg_track_rotation_part_updates: AFTER insert or delete or update, runs `fn_track_rotation_updates`

Foreign keys:

- rotation_participants_rotation_id_fkey: `(rotation_id) REFERENCES rotations(id) ON DELETE CASCADE`
- rotation_participants_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- rotation_participants_pkey (index): `(id)`
- rotation_participants_rotation_id_position_key (index): `(rotation_id, "position")`
- rotation_participants_rotation_id_position_key: `(rotation_id, "position") DEFERRABLE INITIALLY DEFERRED`

**Go:** not ported yet.

### rotation_state

Triggers:

- trg_set_rot_state_pos_on_active_change: BEFORE update when `(new.rotation_participant_id <> old.rotation_participant_id)`, runs `fn_set_rot_state_pos_on_active_change`
- trg_track_rotation_state_updates: AFTER update, runs `fn_track_rotation_updates`

Foreign keys:

- rotation_state_rotation_id_fkey: `(rotation_id) REFERENCES rotations(id) ON DELETE CASCADE`
- rotation_state_rotation_participant_id_fkey: `(rotation_participant_id) REFERENCES rotation_participants(id) DEFERRABLE`

Unique:

- rotation_state_pkey (index): `(rotation_id)`
- rotation_state_uniq_id (index): `(id)`
- rotation_state_uniq_id: `(id)`

**Go:** not ported yet.

### rotations

Triggers:

- trg_track_rotation_updates: AFTER insert or update, runs `fn_track_rotation_updates`

Checks:

- rotations_shift_length_check: `(shift_length > 0)`

Unique:

- rotations_name (index): `(lower(name))`
- rotations_name_unique (index): `(name)`
- rotations_name_unique: `(name)`
- rotations_pkey (index): `(id)`

**Go:** not ported yet.

### schedule_data

Foreign keys:

- schedule_data_schedule_id_fkey: `(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE`

Unique:

- schedule_data_id_key (index): `(id)`
- schedule_data_id_key: `(id)`
- schedule_data_pkey (index): `(schedule_id)`

**Go:** not ported yet.

### schedule_on_call_users

Foreign keys:

- schedule_on_call_users_schedule_id_fkey: `(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE`
- schedule_on_call_users_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Checks:

- schedule_on_call_users_check: `(end_time IS NULL OR end_time > start_time)`

Unique:

- idx_schedule_on_call_once (index): `(schedule_id, user_id) WHERE (end_time IS NULL)`
- schedule_on_call_users_uniq_id (index): `(id)`
- schedule_on_call_users_uniq_id: `(id)`

**Go:** not ported yet.

### schedule_rules

Triggers:

- trg_enforce_schedule_rule_limit: AFTER insert, runs `fn_enforce_schedule_rule_limit`
- trg_enforce_schedule_target_limit: AFTER insert, runs `fn_enforce_schedule_target_limit`

Foreign keys:

- schedule_rules_schedule_id_fkey: `(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE`
- schedule_rules_tgt_rotation_id_fkey: `(tgt_rotation_id) REFERENCES rotations(id) ON DELETE CASCADE`
- schedule_rules_tgt_user_id_fkey: `(tgt_user_id) REFERENCES users(id) ON DELETE CASCADE`

Checks:

- schedule_rules_check: `(tgt_user_id IS NULL AND tgt_rotation_id IS NOT NULL OR tgt_user_id IS NOT NULL AND tgt_rotation_id IS NULL)`

Unique:

- schedule_rules_pkey (index): `(id)`

**Go:** not ported yet.

### schedules

Unique:

- schedules_name (index): `(lower(name))`
- schedules_name_key (index): `(name)`
- schedules_name_key: `(name)`
- schedules_pkey (index): `(id)`

**Go:** not ported yet.

### services

Triggers:

- trg_10_clear_ep_state_on_svc_ep_change: AFTER update when `(old.escalation_policy_id <> new.escalation_policy_id)`, runs `fn_clear_ep_state_on_svc_ep_change`

Foreign keys:

- services_escalation_policy_id_fkey: `(escalation_policy_id) REFERENCES escalation_policies(id)`

Unique:

- services_name (index): `(lower(name))`
- services_name_key (index): `(name)`
- services_name_key: `(name)`
- services_pkey (index): `(id)`
- svc_ep_uniq (index): `(id, escalation_policy_id)`
- svc_ep_uniq: `(id, escalation_policy_id)`

**Go:** not ported yet.

### switchover_log

Unique:

- switchover_log_pkey (index): `(id)`

**Go:** not ported yet.

### switchover_state

Checks:

- switchover_state_ok_check: `(ok)`

Unique:

- switchover_state_pkey (index): `(ok)`

**Go:** not ported yet.

### twilio_sms_callbacks

Foreign keys:

- twilio_sms_callbacks_alert_id_fkey: `(alert_id) REFERENCES alerts(id) ON DELETE CASCADE`
- twilio_sms_callbacks_service_id_fkey: `(service_id) REFERENCES services(id) ON DELETE CASCADE`

Unique:

- idx_twilio_sms_codes (index): `(phone_number, code)`
- twilio_sms_callbacks_uniq_id (index): `(id)`
- twilio_sms_callbacks_uniq_id: `(id)`

**Go:** not ported yet.

### twilio_sms_errors

Unique:

- twilio_sms_errors_uniq_id (index): `(id)`
- twilio_sms_errors_uniq_id: `(id)`

**Go:** not ported yet.

### twilio_voice_errors

Unique:

- twilio_voice_errors_uniq_id (index): `(id)`
- twilio_voice_errors_uniq_id: `(id)`

**Go:** not ported yet.

### uik_config

Foreign keys:

- uik_config_id_fkey: `(id) REFERENCES integration_keys(id) ON DELETE CASCADE`

Unique:

- uik_config_pkey (index): `(id)`
- uik_config_primary_token_key (index): `(primary_token)`
- uik_config_primary_token_key: `(primary_token)`
- uik_config_secondary_token_key (index): `(secondary_token)`
- uik_config_secondary_token_key: `(secondary_token)`

**Go:** not ported yet.

### user_calendar_subscriptions

Triggers:

- trg_enforce_calendar_subscriptions_per_user_limit: AFTER insert, runs `fn_enforce_calendar_subscriptions_per_user_limit`

Foreign keys:

- user_calendar_subscriptions_schedule_id_fkey: `(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE`
- user_calendar_subscriptions_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- user_calendar_subscriptions_name_schedule_id_user_id_key (index): `(name, schedule_id, user_id)`
- user_calendar_subscriptions_name_schedule_id_user_id_key: `(name, schedule_id, user_id)`
- user_calendar_subscriptions_pkey (index): `(id)`

**Go:** not ported yet.

### user_contact_methods

Triggers:

- trg_10_cm_set_dest_on_insert: BEFORE insert when `(new.dest IS NULL)`, runs `fn_cm_set_dest_on_insert`
- trg_10_cm_set_dest_on_update: AFTER update when `((new.dest = old.dest) AND ((new.type <> 'DEST'::enum_user_contact_method_type) AND ((new.value <> old.value) OR (new.type <> old.type))))`, runs `fn_cm_set_dest_on_insert`
- trg_10_compat_set_type_val_on_insert: BEFORE insert when `(new.dest IS NOT NULL)`, runs `fn_cm_compat_set_type_val_on_insert`
- trg_10_compat_set_type_val_on_update: BEFORE update when `(new.dest <> old.dest)`, runs `fn_cm_compat_set_type_val_on_insert`
- trg_cm_set_not_pending_on_verify: BEFORE update of disabled when `((NOT new.disabled) AND old.pending)`, runs `fn_cm_set_not_pending_on_verify`
- trg_enforce_contact_method_limit: AFTER insert, runs `fn_enforce_contact_method_limit`

Foreign keys:

- user_contact_methods_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- user_contact_methods_dest_key (index): `(dest)`
- user_contact_methods_dest_key: `(dest)`
- user_contact_methods_pkey (index): `(id)`
- user_contact_methods_type_value_key (index): `(type, value)`
- user_contact_methods_type_value_key: `(type, value)`

**Go:** not ported yet.

### user_favorites

Foreign keys:

- user_favorites_tgt_escalation_policy_id_fkey: `(tgt_escalation_policy_id) REFERENCES escalation_policies(id) ON DELETE CASCADE`
- user_favorites_tgt_rotation_id_fkey: `(tgt_rotation_id) REFERENCES rotations(id) ON DELETE CASCADE`
- user_favorites_tgt_schedule_id_fkey: `(tgt_schedule_id) REFERENCES schedules(id) ON DELETE CASCADE`
- user_favorites_tgt_service_id_fkey: `(tgt_service_id) REFERENCES services(id) ON DELETE CASCADE`
- user_favorites_tgt_user_id_fkey: `(tgt_user_id) REFERENCES users(id) ON DELETE CASCADE`
- user_favorites_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- user_favorites_uniq_id (index): `(id)`
- user_favorites_uniq_id: `(id)`
- user_favorites_user_id_tgt_escalation_policy_id_key (index): `(user_id, tgt_escalation_policy_id)`
- user_favorites_user_id_tgt_escalation_policy_id_key: `(user_id, tgt_escalation_policy_id)`
- user_favorites_user_id_tgt_rotation_id_key (index): `(user_id, tgt_rotation_id)`
- user_favorites_user_id_tgt_rotation_id_key: `(user_id, tgt_rotation_id)`
- user_favorites_user_id_tgt_schedule_id_key (index): `(user_id, tgt_schedule_id)`
- user_favorites_user_id_tgt_schedule_id_key: `(user_id, tgt_schedule_id)`
- user_favorites_user_id_tgt_service_id_key (index): `(user_id, tgt_service_id)`
- user_favorites_user_id_tgt_service_id_key: `(user_id, tgt_service_id)`
- user_favorites_user_id_tgt_user_id_key (index): `(user_id, tgt_user_id)`
- user_favorites_user_id_tgt_user_id_key: `(user_id, tgt_user_id)`

**Go:** not ported yet.

### user_notification_rules

Triggers:

- trg_enforce_notification_rule_limit: AFTER insert, runs `fn_enforce_notification_rule_limit`
- trg_notification_rule_same_user: BEFORE insert or update, runs `fn_notification_rule_same_user`

Foreign keys:

- user_notification_rules_contact_method_id_fkey: `(contact_method_id) REFERENCES user_contact_methods(id) ON DELETE CASCADE`
- user_notification_rules_user_id_fkey: `(user_id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- user_notification_rules_contact_method_id_delay_minutes_key (index): `(contact_method_id, delay_minutes)`
- user_notification_rules_contact_method_id_delay_minutes_key: `(contact_method_id, delay_minutes)`
- user_notification_rules_pkey (index): `(id)`

**Go:** not ported yet.

### user_overrides

Triggers:

- trg_enforce_user_overide_no_conflict: AFTER insert or update, runs `fn_enforce_user_overide_no_conflict`
- trg_enforce_user_override_schedule_limit: AFTER insert, runs `fn_enforce_user_override_schedule_limit`

Foreign keys:

- user_overrides_add_user_id_fkey: `(add_user_id) REFERENCES users(id) ON DELETE CASCADE`
- user_overrides_remove_user_id_fkey: `(remove_user_id) REFERENCES users(id) ON DELETE CASCADE`
- user_overrides_tgt_schedule_id_fkey: `(tgt_schedule_id) REFERENCES schedules(id) ON DELETE CASCADE`

Checks:

- user_overrides_check1: `(COALESCE(add_user_id, remove_user_id) IS NOT NULL)`
- user_overrides_check2: `(add_user_id <> remove_user_id)`
- user_overrides_check: `(end_time > start_time)`

Unique:

- user_overrides_pkey (index): `(id)`

**Go:** not ported yet.

### user_slack_data

Foreign keys:

- user_slack_data_id_fkey: `(id) REFERENCES users(id) ON DELETE CASCADE`

Unique:

- user_slack_data_pkey (index): `(id)`

**Go:** not ported yet.

### user_verification_codes

Foreign keys:

- user_verification_codes_contact_method_id_fkey: `(contact_method_id) REFERENCES user_contact_methods(id) ON DELETE CASCADE`

Unique:

- user_verification_codes_contact_method_id_key (index): `(contact_method_id)`
- user_verification_codes_contact_method_id_key: `(contact_method_id)`
- user_verification_codes_pkey (index): `(id)`

**Go:** not ported yet.

### users

Triggers:

- trg_enforce_status_update_same_user: BEFORE insert or update, runs `fn_enforce_status_update_same_user`

Foreign keys:

- users_alert_status_log_contact_method_id_fkey: `(alert_status_log_contact_method_id) REFERENCES user_contact_methods(id) ON DELETE SET NULL DEFERRABLE`

Unique:

- goalert_user_pkey (index): `(id)`

**Go:** not ported yet.
