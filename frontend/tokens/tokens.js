// tokens.json: 생성물, 손으로 고치지 않음
function freeze(node) {
  if (typeof node !== 'object') return node;
  return Object.freeze(Object.fromEntries(Object.entries(node).map(([key, value]) => [key, freeze(value)])));
}

/** CSS에 넣을 토큰 참조. 값은 `var(--…)` 문자열이다. */
export const tokens = freeze({
  "font": {
    "mono": "var(--font-mono)"
  },
  "size": {
    "text": {
      "12": "var(--size-text-12)",
      "13": "var(--size-text-13)",
      "14": "var(--size-text-14)"
    },
    "column": {
      "time": "var(--size-column-time)"
    }
  },
  "leading": {
    "normal": "var(--leading-normal)",
    "relaxed": "var(--leading-relaxed)"
  },
  "space": {
    "1": "var(--space-1)",
    "2": "var(--space-2)",
    "3": "var(--space-3)",
    "4": "var(--space-4)",
    "5": "var(--space-5)"
  },
  "border": {
    "thin": "var(--border-thin)"
  },
  "breakpoint": {
    "md": "var(--breakpoint-md)"
  }
});

/** 배치 계산에 쓸 밝은 테마의 실제 값. px와 s·ms만 단위 없는 숫자로 바꾼다. */
export const values = freeze({
  "font": {
    "mono": "'IBM Plex Mono', monospace"
  },
  "size": {
    "text": {
      "12": "0.75rem",
      "13": "0.8125rem",
      "14": "0.875rem"
    },
    "column": {
      "time": 220.0
    }
  },
  "leading": {
    "normal": 1.6,
    "relaxed": 1.7
  },
  "space": {
    "1": 4.0,
    "2": 8.0,
    "3": 12.0,
    "4": 16.0,
    "5": 24.0
  },
  "border": {
    "thin": 1.0
  },
  "breakpoint": {
    "md": 900.0
  }
});
