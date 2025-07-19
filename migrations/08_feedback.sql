create domain rating as int
    constraint rating_gt_zero check (value > 0)
    constraint rating_lt_four check (value < 4);

comment on domain rating is $$
    Feedback rating, scored 1-3. 1 for negative, 2 for neutral, 3 for positive.
$$;

create table seller_feedback
(
    checkout_id int,
    seller_id   int,

    rating      rating      not null,
    comment     text
        constraint comment_not_empty check (trim(comment) <> ''),

    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now(),

    foreign key (checkout_id, seller_id) references transaction (checkout_id, seller_id),
    primary key (checkout_id, seller_id)
);

comment on table seller_feedback is $$
    Feedback from buyer to seller. Feedback can only be left by a user who purchased an item from the seller.
    Only one piece of feedback can be left per purchase from the seller (rather than per item).
$$;