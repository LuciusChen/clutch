# 207 — timestamptz Values Carry the Local Offset

## Evidence

Clutch shows a PostgreSQL `timestamptz` in Emacs's local time without an offset: `pgsql.el` decodes it to a Lisp time, and the adapter decodes that in Emacs's time zone. Values went back without an offset too, and PostgreSQL reads such text in the session time zone, which neither `pgsql.el` nor Clutch sets. With a server in UTC and Emacs in Asia/Shanghai, on Clutch 0.5.3, a value stored as 03:04:05 UTC showed as 11:04:05; editing it to 11:04:06 stored 11:04:06 UTC, eight hours later than shown, and 12:00:00 entered in the insert form showed as 20:00:00 after a refresh. With the session set to Emacs's offset, the same edit stored 03:04:06 UTC. A value read from the server and sent back compared unequal to the stored time, so a `timestamptz` key matched no row. The preview showed the same text that was sent.

## Decision

- A `timestamptz` parameter whose text is a date and time without an offset, whether typed or formatted from a result, gains the UTC offset that Emacs's time zone has at that time, as in `+08:00`. This happens where the adapter encodes parameters and where it renders them as literals, so the preview and a copied `INSERT` show what is sent. Text with an offset, and other text such as `infinity`, is sent as written.
- The session time zone is left alone. Setting it to Emacs's zone on connecting would change what the user's own queries return, such as `now()::text`, and Emacs may know its zone only by an abbreviation such as `CST`.
- Showing `timestamptz` values with their offset would keep the exact instant through an edit, but it would change every grid, export and filter, and the validation of the insert form, so it was not done here.

## Limits

- During the hour that a change from daylight saving time repeats, a shown time is taken as standard time, so the first of the two instants moves an hour when it is edited.
- Text inside a `timestamptz[]` array is sent as written, since a curly-brace literal is passed through unchanged.
- Following a foreign key renders the value without its type, so a `timestamptz` foreign key is still read in the session time zone.

## Verification

- A unit test sends and previews offset-less text and a result value with New York's winter and summer offsets, and leaves text with an offset, `infinity` and a `timestamp` value as they are. It fails on 0.5.3.
- A live test against PostgreSQL 16, with the session in UTC and Emacs in Asia/Shanghai, sends back a value read from the server and an edited one: the first compares equal to the stored time, and the second is stored as the time shown. It fails on 0.5.3.
