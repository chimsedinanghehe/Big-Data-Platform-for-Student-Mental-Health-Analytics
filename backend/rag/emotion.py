from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from backend.rag.config import RAGSettings, get_settings


DEFAULT_LABELS = (
    "ANGER_DISGUST",
    "DESPAIR_AND_GRIEF",
    "FEAR_NERVOUSNESS",
    "NEUTRAL",
    "POSITIVE",
    "SURPRISE_CURIOSITY",
)

NEGATIVE_EMOTION_LABELS = {
    "ANGER_DISGUST",
    "DESPAIR_AND_GRIEF",
    "FEAR_NERVOUSNESS",
}


@dataclass(frozen=True)
class EmotionSignal:
    label: str
    confidence: float
    detected: bool
    intensity: str
    top_k: list[dict[str, float | str]]
    negative_flag: bool
    model: str
    threshold: float


@lru_cache(maxsize=2)
def _load_classifier(model_path: str):
    path = Path(model_path).expanduser().resolve()
    if not path.exists():
        raise RuntimeError(f"Emotion model path does not exist: {path}")

    tokenizer = AutoTokenizer.from_pretrained(str(path))
    model = AutoModelForSequenceClassification.from_pretrained(str(path))
    model.eval()
    return tokenizer, model


def classify_emotion(text: str, settings: RAGSettings | None = None, top_k: int = 3) -> EmotionSignal | None:
    settings = settings or get_settings()
    if not settings.emotion_model_path:
        return None

    tokenizer, model = _load_classifier(settings.emotion_model_path)
    inputs = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=128,
        padding="max_length",
    )

    with torch.no_grad():
        outputs = model(**inputs)

    probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)[0]
    top_count = min(top_k, len(probabilities))
    top_values, top_indices = torch.topk(probabilities, k=top_count)
    top_predictions = [
        {
            "label": _resolve_label(model, int(label_index)),
            "confidence": float(confidence),
        }
        for confidence, label_index in zip(top_values, top_indices, strict=True)
    ]

    label = str(top_predictions[0]["label"])
    confidence = float(top_predictions[0]["confidence"])
    return EmotionSignal(
        label=label,
        confidence=confidence,
        detected=confidence >= settings.emotion_confidence_threshold,
        intensity=_emotion_intensity(confidence),
        top_k=top_predictions,
        negative_flag=label in NEGATIVE_EMOTION_LABELS,
        model=Path(settings.emotion_model_path).name,
        threshold=settings.emotion_confidence_threshold,
    )


def format_emotion_signal(signal: EmotionSignal | None) -> str:
    if signal is None or not signal.detected:
        return "No high-confidence emotional signal detected."
    confidence_percent = round(signal.confidence * 100, 1)
    return f"{signal.label} ({confidence_percent}% confidence)"


def emotion_signal_to_metadata(signal: EmotionSignal | None) -> dict:
    if signal is None:
        return {
            "label": None,
            "confidence": None,
            "detected": False,
            "intensity": None,
            "negative_flag": False,
            "model": None,
            "threshold": None,
        }

    return {
        "label": signal.label,
        "confidence": signal.confidence,
        "detected": signal.detected,
        "intensity": signal.intensity,
        "negative_flag": signal.negative_flag,
        "model": signal.model,
        "threshold": signal.threshold,
    }


def _resolve_label(model, label_index: int) -> str:
    label = model.config.id2label.get(label_index, str(label_index))
    if label.startswith("LABEL_") and label_index < len(DEFAULT_LABELS):
        return DEFAULT_LABELS[label_index]
    return label


def _emotion_intensity(confidence: float) -> str:
    if confidence >= 0.85:
        return "high"
    if confidence >= 0.6:
        return "medium"
    return "low"
