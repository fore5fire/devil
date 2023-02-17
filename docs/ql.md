# Courier Query Language

The courier query language is a series of statements which define network
messages to run and how to run them. Statements either consist of a bare
step or a command.

## Steps

A step specifies a message to be sent. If a statement consists of just a step
definition with no command, then it also sends the message immediately. Steps
consist of a header, followed by the payload which uses a kind-specific syntax,
then a newline and an EOF token.

The Step header follows the form
```
<kind> [name] <EOF-token>
```

For example:
```
https ---
GET example.com/user/123
---
```
Or to give the same step a name:
```
https get_user ---
GET example.com/user/123
---
```

Whatever EOF token you define in a step's header will be used to determine the
end of the step body. Any UTF-8 string is allowed but it cannot contain newline
characters (\r or \n). The EOF token always goes on its own line, which means if
you want the step body to include a trailing newline there should be an empty
line before the EOF token.

```
http put_user ======EOF
PUT example.com/user/123
foo

======EOF
```

### HTTP and HTTPS

### GraphQL

### Websockets

### File

## Variables and special literals

## Commands

### while

### for

### define
