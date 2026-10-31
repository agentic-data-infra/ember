# Ember's optional parts, off by default. Turn one on with e.g.
# `buck2 build -c ember.stoke=true //...`.

# stoke (third-party/stoke, the Buck cell stoke//): the stoke_tokenize and
# stoke_pack functions (ember/common/Stoke.h).
EMBER_STOKE = read_config("ember", "stoke", "false") == "true"

OPTION_FLAGS = ["-DEMBER_STOKE=1"] if EMBER_STOKE else []
