# Pseudo filter plugin for Embulk
## Overview
Does cryptographic hashing for given columns. An input value is replaced with a value that has been encrypted and hashed using Hash-based Message Authentication Code (HMAC)-Secure Hash Algorithm (SHA)-256.

This can be useful when you need to do pseudonymization for data.

* **Plugin type**: filter

## Configuration

- **key**: secret key (string, required)
- **column_names**: names of string columns to encrypt (array of string, required)

## Example

```yaml
filters:
  - type: pseudo
    column_names: [ip]
    key: super secret key
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
