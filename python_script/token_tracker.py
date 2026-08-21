"""Compteur global de tokens partagé entre tous les modules LLM."""

usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

def track(response):
    """À appeler juste après chaque client.chat.completions.create(...)"""
    if response and response.usage:
        usage["prompt_tokens"] += response.usage.prompt_tokens
        usage["completion_tokens"] += response.usage.completion_tokens
        usage["total_tokens"] += response.usage.total_tokens

def reset():
    usage["prompt_tokens"] = 0
    usage["completion_tokens"] = 0
    usage["total_tokens"] = 0

def summary() -> str:
    return (
        f"Tokens entrée : {usage['prompt_tokens']}\n"
        f"Tokens sortie : {usage['completion_tokens']}\n"
        f"Total : {usage['total_tokens']}"
    )