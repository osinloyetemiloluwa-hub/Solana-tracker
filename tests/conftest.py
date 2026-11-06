import discord


def _field(self, name):
    """Value of the embed field called `name` (helper used across the tests)."""
    for field in self.fields:
        if field.name == name:
            return field.value
    raise KeyError(name)


discord.Embed.f = _field
