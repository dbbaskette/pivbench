drop table if exists reason;
create table reason
(
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)
) distributed by (r_reason_sk);