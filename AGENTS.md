<!-- BEGIN:nextjs-agent-rules -->
# This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` before writing any code. Heed deprecation notices.
<!-- END:nextjs-agent-rules -->

# 🤖 AI Agent Coding Rules & Harness Guidelines

This repository follows a strict verification-first development cycle. All AI agents working on this project MUST strictly adhere to the following rules:

---

## 📋 핵심 코딩 및 검증 규칙 (Core Rules)

1. **소비자 UX 관점의 종단간(E2E) 테스트**
   * 단순 코드 작성이나 컴파일 통과에 만족하지 않고, 소비자가 실제 기능을 사용하는 동선 그대로 처음부터 끝까지 완전하게 작동하는지 테스트한다.

2. **수정 시 무조건 테스트 수행**
   * 한 라인이라도 소스코드를 수정했거나 UI를 변경했다면, 반드시 즉각적으로 영향을 받는 범위와 전체 시스템의 동작을 테스트해야 한다.

3. **브라우저 직접 확인**
   * GUI나 화면 레이아웃과 관련된 변경은 이론적 예측에만 의존하지 않는다. 실제 로컬 개발 서버를 구동하고 브라우저를 띄워 렌더링, 정렬, 반응형 동작 등을 실시간 화면으로 직접 확인한다.

4. **실패 시 끝까지 원인 추적 및 재수정**
   * 테스트나 확인 과정에서 오작동 혹은 의도치 않은 레이아웃이 발견되면, 추측성 패치를 반복하지 않고 로그/콘솔/트레이스를 분석하여 정확한 원인을 진단한 뒤 완벽하게 다시 고친다.

5. **오답 노트 및 실수 반복 금지 기록**
   * 이전에 겪은 시행착오나 특정 라이브러리/디렉터리 구조의 고유 특성, 개발 도중의 실수를 기록하여 이후 동일한 오류를 범하지 않도록 문서로 남긴다.

6. **역할 분담 및 에이전트 협업**
   * 작업 단위가 거대하거나 복잡한 영역(예: 백엔드 스크래핑 로직 고도화 vs 프론트엔드 차트 애니메이션 등)은 혼자서 무리하게 진행하지 않고, 역할을 명확히 쪼개어 다른 서브에이전트(AI)에게 분담을 맡겨 최적의 완성도를 이끌어낸다.

---

## 🚀 Git Push 전 자가진단 프로세스 (Pre-push Verification Checklist)

사용자가 **푸시(Push) 명령**을 내리거나 승인할 때 에이전트는 즉시 다음 체크리스트를 실행하며, 모든 항목에 문제가 없을 때만 최종 푸시 작업을 수행한다.

- [ ] **Type & Compile Check**: `npx tsc --noEmit`을 실행하여 TypeScript 타입 오류나 문법 에러가 전혀 없는지 확인했는가?
- [ ] **Build Check**: `npm run build`가 도중에 중단되거나 경고를 뿜지 않고 깔끔하게 성공하는가?
- [ ] **UX & Render Check**: 변경한 화면(특히 모바일/PC 반응형 헤더 및 버튼)이 브라우저에서 올바르게 배치되고 가독성이 훌륭한가?
- [ ] **E2E Flow Check**: 필터를 변경하고, 데이터를 로딩하고, 상세 모달(이평선 차트)을 열고, 엑셀을 다운로드하는 등 핵심 사용자 경험 시나리오가 끊김 없이 매끄럽게 작동하는가?
- [ ] **Issue Log Verification**: 콘솔 및 에러 로그상에 빨간 경고나 예외가 한 개도 남지 않았음을 직접 확인했는가?

---

## 🛠 빌드 및 런타임 명령어 요약

* **로컬 개발 서버 실행**: `npm run dev`
* **TypeScript 검증**: `npx tsc --noEmit`
* **빌드 수행**: `npm run build`
