# Курсовая работа — Задание 2: Mini RxJava

Собственная реализация реактивной библиотеки в стиле RxJava с поддержкой Observer/Observable, операторов преобразования, Schedulers и отмены подписок.

## Структура проекта

```
src/main/java/ru/mephi/rxjava/
├── Observer.java, Observable.java, Disposable.java
├── Scheduler.java, Schedulers.java
├── Function.java, Predicate.java
├── ObservableOnSubscribe.java, ObservableEmitter.java
├── internal/
│   ├── SafeObserver.java
│   ├── ConnectionObserver.java
│   ├── CreateEmitter.java
│   ├── SerialDisposable.java
│   └── DisposableHelper.java
├── operators/
│   ├── ObservableCreate.java
│   ├── ObservableMap.java
│   ├── ObservableFilter.java
│   ├── ObservableFlatMap.java
│   ├── ObservableSubscribeOn.java
│   └── ObservableObserveOn.java
└── schedulers/
    ├── IOThreadScheduler.java
    ├── ComputationScheduler.java
    └── SingleThreadScheduler.java

src/test/java/ru/mephi/rxjava/
├── ObservableCreateTest.java
├── OperatorTest.java
├── SchedulerTest.java
└── DisposableTest.java
```

## Сборка и тесты

```bash
mvn test
```

Без Maven:

```bash
javac -d target/classes src/main/java/ru/mephi/rxjava/**/*.java ...
```

---

## Отчёт

### 1. Архитектура системы

Библиотека построена вокруг паттерна **Наблюдатель (Observer)**:

| Компонент | Роль |
|-----------|------|
| `Observable<T>` | Источник асинхронного потока данных |
| `Observer<T>` | Потребитель событий: `onNext`, `onError`, `onComplete` |
| `Disposable` | Отмена подписки |
| `Scheduler` | Выполнение задач в заданном пуле потоков |

**Цепочка подписки:**

```
subscribe() → SafeObserver → операторы → ObservableCreate → CreateEmitter
```

- `Observable.create()` принимает `ObservableOnSubscribe` и создаёт `ObservableCreate`.
- Каждый оператор (`map`, `filter`, `flatMap`, `subscribeOn`, `observeOn`) — отдельный подкласс `Observable`, делегирующий подписку источнику.
- `ConnectionObserver` связывает upstream/downstream и обеспечивает каскадную отмену через `SerialDisposable`.
- `SafeObserver` защищает от повторных терминальных событий и null-значений.

### 2. Операторы преобразования

| Оператор | Назначение |
|----------|------------|
| `map` | Преобразует каждый элемент `T → R` |
| `filter` | Пропускает элементы по предикату |
| `flatMap` | Разворачивает вложенные `Observable` в единый поток |
| `subscribeOn` | Подписка на источник в потоке Scheduler |
| `observeOn` | Доставка событий Observer в потоке Scheduler |

**flatMap:** для каждого элемента источника создаётся внутренний `Observable`, на который оформляется подписка. Счётчик `activeCount` отслеживает активные внутренние потоки; `onComplete` downstream вызывается, когда источник завершён и все внутренние потоки тоже.

### 3. Schedulers — принципы и применение

| Scheduler | Реализация | Аналог RxJava | Когда использовать |
|-----------|------------|---------------|-------------------|
| `IOThreadScheduler` | `CachedThreadPool` | `Schedulers.io()` | Сетевые запросы, файловый I/O, блокирующие операции |
| `ComputationScheduler` | `FixedThreadPool` (по числу ядер) | `Schedulers.computation()` | CPU-bound вычисления, обработка данных |
| `SingleThreadScheduler` | `SingleThreadExecutor` | `Schedulers.single()` | Последовательные задачи, где важен порядок |

**subscribeOn vs observeOn:**

- `subscribeOn` — определяет, **в каком потоке произойдёт подписка** на источник (вызов `subscribeActual`).
- `observeOn` — определяет, **в каком потоке Observer получит** `onNext` / `onError` / `onComplete`.

Типичный паттерн для UI/сервера:

```java
Observable.create(emitter -> fetchFromNetwork(emitter))
    .subscribeOn(Schedulers.io())           // сеть — в IO-потоке
    .map(this::parseResponse)               // парсинг — тоже в IO
    .observeOn(Schedulers.computation())    // обработка — в computation
    .subscribe(observer);
```

**Различия:**
- `IO` создаёт потоки по требованию (неограниченно) — хорош для блокирующего I/O, но опасен при неконтролируемой нагрузке.
- `Computation` ограничен числом ядер — подходит для вычислений, не для блокирующих вызовов.
- `Single` гарантирует строгую последовательность — удобен для записи в общий буфер без гонок.

### 4. Обработка ошибок и Disposable

- Любое исключение в `map`/`filter`/`flatMap` передаётся в `onError`.
- `SafeObserver` гарантирует единственный терминальный callback.
- `Disposable.dispose()` каскадно отменяет всю цепочку подписок.
- `CreateEmitter` после `dispose()` игнорирует дальнейшие `onNext`.

### 5. Примеры использования

**Базовый поток:**

```java
Observable.create(emitter -> {
    emitter.onNext("hello");
    emitter.onNext("world");
    emitter.onComplete();
}).subscribe(new Observer<String>() {
    public void onNext(String item) { System.out.println(item); }
    public void onError(Throwable t) { t.printStackTrace(); }
    public void onComplete() { System.out.println("done"); }
});
```

**Операторы:**

```java
Observable.create(emitter -> {
    for (int i = 1; i <= 5; i++) emitter.onNext(i);
    emitter.onComplete();
})
.filter(n -> n % 2 == 0)
.map(n -> "even:" + n)
.subscribe(observer);
```

**flatMap:**

```java
Observable.create(emitter -> {
    emitter.onNext("user-1");
    emitter.onComplete();
})
.flatMap(id -> Observable.create(e -> {
    e.onNext(id + "/profile");
    e.onNext(id + "/orders");
    e.onComplete();
}))
.subscribe(observer);
```

**Schedulers + отмена:**

```java
Disposable subscription = Observable.create(emitter -> {
        loadData(emitter);
    })
    .subscribeOn(Schedulers.io())
    .observeOn(Schedulers.single())
    .subscribe(observer);

// позже:
subscription.dispose();
```

### 6. Тестирование

| Тест-класс | Что проверяет |
|------------|---------------|
| `ObservableCreateTest` | эмиссия элементов, `onComplete`, `onError` |
| `OperatorTest` | `map`, `filter`, `flatMap`, ошибки в операторах |
| `SchedulerTest` | `subscribeOn`/`observeOn` на IO, Computation, Single |
| `DisposableTest` | отмена изнутри emitter и снаружи через `Disposable` |

Тесты используют `CountDownLatch` для синхронизации асинхронных событий и проверяют имена потоков для Schedulers.

**Запуск:**

```bash
mvn test
```

---

## Автор

Курсовая работа по дисциплине «Многопоточное и асинхронное программирование на Java».
