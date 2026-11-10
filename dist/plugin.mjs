import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import * as Path from "node:path";
import * as Fs from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Pipeable.js
/**
* The `Pipeable` module defines the shared interface and implementation helpers
* for values that support Effect-style method chaining with `.pipe(...)`.
*
* A `Pipeable` value can pass itself through a sequence of unary functions from
* left to right, so code can be written as `value.pipe(f, g, h)` instead of
* deeply nesting calls. This is the method form used by many Effect data types
* to compose transformations, validations, and effectful operations while
* keeping the original value as the starting point of the pipeline.
*
* @stability stable
* @since 2.0.0
*/
/**
* Applies a `pipe` method's variadic arguments to an initial value from left
* to right.
*
* **When to use**
*
* Use to implement a custom `.pipe(...)` method from JavaScript's `arguments`
* object.
*
* **Details**
*
* This helper is intended for implementing `Pipeable.pipe` methods that
* receive JavaScript's `arguments` object. With no functions it returns the
* original value; otherwise it feeds each result into the next function.
*
* **Example** (Implementing a pipe method)
*
* ```ts import.meta.vitest
* import { Pipeable } from "effect"
*
* class NumberBox {
*   constructor(readonly value: number) {}
*
*   pipe(..._fns: ReadonlyArray<(value: number) => number>): number {
*     return Pipeable.pipeArguments(this.value, arguments) as number
*   }
* }
*
* const result = new NumberBox(5).pipe(
*   (n) => n + 2,
*   (n) => n * 3
* )
* result // => 21
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const pipeArguments = (self, args) => {
	switch (args.length) {
		case 0: return self;
		case 1: return args[0](self);
		case 2: return args[1](args[0](self));
		case 3: return args[2](args[1](args[0](self)));
		case 4: return args[3](args[2](args[1](args[0](self))));
		case 5: return args[4](args[3](args[2](args[1](args[0](self)))));
		case 6: return args[5](args[4](args[3](args[2](args[1](args[0](self))))));
		case 7: return args[6](args[5](args[4](args[3](args[2](args[1](args[0](self)))))));
		case 8: return args[7](args[6](args[5](args[4](args[3](args[2](args[1](args[0](self))))))));
		case 9: return args[8](args[7](args[6](args[5](args[4](args[3](args[2](args[1](args[0](self)))))))));
		default: {
			let ret = self;
			for (let i = 0, len = args.length; i < len; i++) ret = args[i](ret);
			return ret;
		}
	}
};
/**
* Reusable prototype that implements `Pipeable.pipe`.
*
* **When to use**
*
* Use when classes or object prototypes can reuse this value when they need the
* standard pipe implementation backed by `pipeArguments`.
*
* @stability stable
* @category prototypes
* @since 3.15.0
*/
const Prototype$1 = { pipe() {
	return pipeArguments(this, arguments);
} };
/**
* Provides a base constructor whose instances implement the standard `Pipeable.pipe`
* method.
*
* **When to use**
*
* Use when you need to define a class that supports Effect-style method
* chaining through `.pipe(...)`.
*
* @stability stable
* @category constructors
* @since 3.15.0
*/
const Class$1 = /*#__PURE__*/ function() {
	function PipeableBase() {}
	PipeableBase.prototype = Prototype$1;
	return PipeableBase;
}();
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Function.js
/**
* Creates a function that can be called in data-first style or data-last
* (`pipe`-friendly) style.
*
* **When to use**
*
* Use to expose one implementation through both direct and `pipe`-friendly
* call styles.
*
* **Details**
*
* Pass either the arity of the uncurried function or a predicate that decides
* whether the current call is data-first. Arity is the common case. Use a
* predicate when optional arguments make arity ambiguous.
*
* **Example** (Selecting data-first or data-last style by arity)
*
* ```ts import.meta.vitest
* import { Function, pipe } from "effect"
*
* const sum = Function.dual<
*   (that: number) => (self: number) => number,
*   (self: number, that: number) => number
* >(2, (self, that) => self + that)
*
* sum(2, 3) // => 5
* pipe(2, sum(3)) // => 5
* ```
*
* **Example** (Defining overloads with call signatures)
*
* ```ts import.meta.vitest
* import { Function, pipe } from "effect"
*
* const sum: {
*   (that: number): (self: number) => number
*   (self: number, that: number): number
* } = Function.dual(2, (self: number, that: number): number => self + that)
*
* sum(2, 3) // => 5
* pipe(2, sum(3)) // => 5
* ```
*
* **Example** (Selecting data-first or data-last style with a predicate)
*
* ```ts import.meta.vitest
* import { Function, pipe } from "effect"
*
* const sum = Function.dual<
*   (that: number) => (self: number) => number,
*   (self: number, that: number) => number
* >(
*   (args) => args.length === 2,
*   (self, that) => self + that
* )
*
* sum(2, 3) // => 5
* pipe(2, sum(3)) // => 5
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const dual = function(arity, body) {
	if (typeof arity === "function") return function() {
		return arity(arguments) ? body.apply(this, arguments) : (self) => body(self, ...arguments);
	};
	switch (arity) {
		case 0:
		case 1: throw new RangeError(`Invalid arity ${arity}`);
		case 2: return function(a, b) {
			if (arguments.length >= 2) return body(a, b);
			return function(self) {
				return body(self, a);
			};
		};
		case 3: return function(a, b, c) {
			if (arguments.length >= 3) return body(a, b, c);
			return function(self) {
				return body(self, a, b);
			};
		};
		default: return function() {
			if (arguments.length >= arity) return body.apply(this, arguments);
			const args = arguments;
			return function(self) {
				return body(self, ...args);
			};
		};
	}
};
/**
* Returns its input argument unchanged.
*
* **When to use**
*
* Use to return a value unchanged where a function is required.
*
* **Example** (Returning the same value)
*
* ```ts import.meta.vitest
* import { identity } from "effect"
*
* identity(5) // => 5
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const identity$1 = (a) => a;
/**
* Creates a zero-argument function that always returns the provided value.
*
* **When to use**
*
* Use when you need a thunk or callback that returns the same value on every
* invocation.
*
* **Example** (Creating a constant thunk)
*
* ```ts import.meta.vitest
* import { Function } from "effect"
*
* const constNull = Function.constant(null)
*
* constNull() // => null
* constNull() // => null
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const constant = (value) => () => value;
/**
* Returns `true` when called.
*
* **When to use**
*
* Use when you need a thunk that returns `true` on every invocation.
*
* **Example** (Returning true from a thunk)
*
* ```ts import.meta.vitest
* import { Function } from "effect"
*
* Function.constTrue() // => true
* ```
*
* @stability stable
* @category constants
* @since 2.0.0
*/
const constTrue = /*#__PURE__*/ constant(true);
/**
* Returns `undefined` when called.
*
* **When to use**
*
* Use when you need a thunk that returns `undefined` on every invocation.
*
* **Example** (Returning undefined from a thunk)
*
* ```ts import.meta.vitest
* import { Function } from "effect"
*
* Function.constUndefined() // => undefined
* ```
*
* @stability stable
* @category constants
* @since 2.0.0
*/
const constUndefined = /*#__PURE__*/ constant(void 0);
/**
* Returns no meaningful value when called.
*
* **When to use**
*
* Use when you need a thunk that is called only for its effect and has no
* meaningful return value.
*
* **Example** (Returning void from a thunk)
*
* ```ts import.meta.vitest
* import { Function } from "effect"
*
* Function.constVoid() // => undefined
* ```
*
* @stability stable
* @category constants
* @since 2.0.0
*/
const constVoid = constUndefined;
/**
* Creates a memoized function whose input is an object, caching results by
* object identity.
*
* **When to use**
*
* Use to reuse the result of a synchronous computation whose output is stable
* for a given object reference.
*
* **Details**
*
* Each memoized wrapper owns a private `WeakMap` keyed by object identity.
*
* **Gotchas**
*
* `undefined` is reserved to represent a cache miss and is therefore not
* supported as a return value.
*
* Structurally equal objects do not share cache entries. If the same object is
* mutated after its first call, later calls still return the cached result for
* that reference.
*
* @stability stable
* @category caching
* @since 4.0.0
*/
function memoize(f) {
	const cache = /* @__PURE__ */ new WeakMap();
	return (a) => {
		const cached = cache.get(a);
		if (cached !== void 0) return cached;
		const result = f(a);
		cache.set(a, result);
		return result;
	};
}
/**
* Creates a memoized idempotent object transformation that caches both inputs
* and their outputs by object identity.
*
* **When to use**
*
* Use when an object transformation is idempotent and its output can be safely
* reused as a fixed point.
*
* **Details**
*
* After computing an input, the returned function caches both the input and
* the output. Calling it with either reference returns the output without
* invoking the supplied function again.
*
* **Gotchas**
*
* The returned function treats each computed output as a fixed point. If
* applying the supplied function to an output would produce an observably
* different value, this memoization changes that behavior.
*
* @see {@link memoize} for memoizing functions without an idempotence requirement
* @stability stable
* @category caching
* @since 4.0.0
*/
function memoizeIdempotent(f) {
	const cache = /* @__PURE__ */ new WeakMap();
	return (a) => {
		const cached = cache.get(a);
		if (cached !== void 0) return cached;
		const result = f(a);
		cache.set(a, result);
		if (result !== a) cache.set(result, result);
		return result;
	};
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/equal.js
/** @internal */
const getAllObjectKeys = (obj) => {
	const keys = new Set(Reflect.ownKeys(obj));
	if (obj.constructor === Object) return keys;
	if (obj instanceof Error) keys.delete("stack");
	const proto = Object.getPrototypeOf(obj);
	let current = proto;
	while (current !== null && current !== Object.prototype) {
		const ownKeys = Reflect.ownKeys(current);
		for (let i = 0; i < ownKeys.length; i++) keys.add(ownKeys[i]);
		current = Object.getPrototypeOf(current);
	}
	if (keys.has("constructor") && typeof obj.constructor === "function" && proto === obj.constructor.prototype) keys.delete("constructor");
	return keys;
};
/** @internal */
const byReferenceInstances = /*#__PURE__*/ new WeakSet();
/** @internal */
const viewBytes = (view) => new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
/** @internal */
const isArrayBufferLike = (u) => u instanceof ArrayBuffer || typeof SharedArrayBuffer !== "undefined" && u instanceof SharedArrayBuffer;
/**
* Returns `undefined` for detached buffers, which can no longer be read.
*
* @internal
*/
const bufferBytes = (buffer) => {
	try {
		return new Uint8Array(buffer);
	} catch {
		return;
	}
};
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/hash.js
/**
* Back-edge count used to avoid caching entry-point-dependent hashes.
*
* @internal
*/
let backEdges = 0;
/** @internal */
const addBackEdge = () => {
	backEdges++;
};
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Predicate.js
/**
* Defines runtime checks for values.
*
* A `Predicate<A>` returns `true` or `false` for an `A`. A
* `Refinement<A, B>` is a predicate that also narrows the TypeScript type when
* it succeeds. This module includes guards for common JavaScript values,
* property and tag checks, tuple and struct checks, boolean combinators, and
* helpers for composing predicates and refinements.
*
* @stability stable
* @since 2.0.0
*/
/**
* Checks whether a value is a `string`.
*
* **When to use**
*
* Use when you need a `Predicate` guard to narrow an `unknown` value to a
* string.
*
* **Details**
*
* Uses `typeof input === "string"`.
*
* **Example** (Guarding strings)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = "hi"
*
* if (Predicate.isString(data)) {
*   data.toUpperCase() // => "HI"
* }
* ```
*
* @see {@link isNumber}
* @see {@link isBoolean}
* @see {@link Refinement}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isString$1(input) {
	return typeof input === "string";
}
/**
* Checks whether a value is a `number`.
*
* **When to use**
*
* Use when you need a `Predicate` guard to narrow an `unknown` value to a
* number.
*
* **Details**
*
* Uses `typeof input === "number"` and does not exclude `NaN` or `Infinity`.
*
* **Example** (Guarding numbers)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = 42
*
* if (Predicate.isNumber(data)) {
*   data + 1 // => 43
* }
* ```
*
* @see {@link isBigInt}
* @see {@link isString}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isNumber(input) {
	return typeof input === "number";
}
/**
* Checks whether a value is a `boolean`.
*
* **When to use**
*
* Use when you need a `Predicate` guard to narrow an `unknown` value to a
* boolean.
*
* **Details**
*
* Uses `typeof input === "boolean"`.
*
* **Example** (Guarding booleans)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = true
*
* if (Predicate.isBoolean(data)) {
*   data ? "yes" : "no" // => "yes"
* }
* ```
*
* @see {@link isString}
* @see {@link isNumber}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isBoolean(input) {
	return typeof input === "boolean";
}
/**
* Checks whether a value is a `symbol`.
*
* **When to use**
*
* Use when you need a `Predicate` guard to narrow an `unknown` value to a
* symbol.
*
* **Details**
*
* Uses `typeof input === "symbol"`.
*
* **Example** (Guarding symbols)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = Symbol.for("id")
*
* if (Predicate.isSymbol(data)) {
*   data.description // => "id"
* }
* ```
*
* @see {@link isPropertyKey}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isSymbol(input) {
	return typeof input === "symbol";
}
/**
* Checks whether a value is a valid `PropertyKey` (string, number, or symbol).
*
* **When to use**
*
* Use when you need a `Predicate` guard for unknown property keys before
* indexing.
*
* **Details**
*
* Uses `isString`, `isNumber`, and `isSymbol`.
*
* **Example** (Guarding property keys)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const key: unknown = "name"
* const obj: Record<PropertyKey, unknown> = { name: "Ada" }
*
* if (Predicate.isPropertyKey(key) && key in obj) {
*   obj[key] // => "Ada"
* }
* ```
*
* @see {@link isString}
* @see {@link isNumber}
* @see {@link isSymbol}
* @stability stable
* @category guards
* @since 4.0.0
*/
function isPropertyKey(u) {
	return isString$1(u) || isNumber(u) || isSymbol(u);
}
/**
* Checks whether a value is a `function`.
*
* **When to use**
*
* Use when you need a `Predicate` guard to narrow an `unknown` value to a
* callable function.
*
* **Details**
*
* Uses `typeof input === "function"`.
*
* **Example** (Guarding functions)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = () => 1
*
* if (Predicate.isFunction(data)) {
*   data() // => 1
* }
* ```
*
* @see {@link isObjectKeyword}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isFunction(input) {
	return typeof input === "function";
}
/**
* Checks whether a value is not `null` and not `undefined`.
*
* **When to use**
*
* Use when you need a `Predicate` refinement that filters out nullish values
* but keeps other falsy ones.
*
* **Details**
*
* Uses `input != null`.
*
* **Example** (Filtering non-nullish values)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const values = [0, null, "", undefined]
* const present = values.filter(Predicate.isNotNullish) // => [0, ""]
* ```
*
* @see {@link isNullish}
* @see {@link isNotNull}
* @see {@link isNotUndefined}
* @stability stable
* @category guards
* @since 4.0.0
*/
function isNotNullish(input) {
	return input != null;
}
/**
* Type guard that always returns `true`.
*
* **When to use**
*
* Use when you need a `Predicate` that always accepts, e.g. as a placeholder.
*
* **Example** (Matching every value)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* Predicate.isUnknown(123) // => true
* ```
*
* @see {@link isNever}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isUnknown(_) {
	return true;
}
/**
* Checks whether a value is a non-null object value that is not an array.
*
* **When to use**
*
* Use to narrow unknown input to a non-null, non-array object with a
* `Predicate` guard.
*
* **Details**
*
* This is a structural runtime check using `typeof input === "object"`, so it
* also accepts object instances such as `Date`, `Map`, class instances, and
* typed arrays. It excludes `null` and arrays.
*
* **Example** (Guarding objects)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* Predicate.isObject({ a: 1 }) // => true
* Predicate.isObject([1, 2]) // => false
* ```
*
* @see {@link isObjectOrArray}
* @see {@link isReadonlyObject}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isObject$1(input) {
	return typeof input === "object" && input !== null && !Array.isArray(input);
}
/**
* Checks whether a value is an `object` in the JavaScript sense (objects, arrays, functions).
*
* **When to use**
*
* Use when you need a `Predicate` guard that accepts arrays and functions as
* well as objects.
*
* **Details**
*
* Returns `true` for arrays and functions, and `false` for `null`.
*
* **Example** (Checking object keywords)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* Predicate.isObjectKeyword(() => 1) // => true
* Predicate.isObjectKeyword(null) // => false
* ```
*
* @see {@link isObject}
* @see {@link isObjectOrArray}
* @stability stable
* @category guards
* @since 4.0.0
*/
function isObjectKeyword(input) {
	return typeof input === "object" && input !== null || isFunction(input);
}
/**
* Checks whether a value has a given property key.
*
* **When to use**
*
* Use when you need a `Predicate` guard for property access on `unknown`
* values with a simple structural object check.
*
* **Details**
*
* Uses the `in` operator and `isObjectKeyword`. This does not check property
* value types.
*
* **Example** (Guarding object properties)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const hasName = Predicate.hasProperty("name")
* const data: unknown = { name: "Ada" }
*
* if (hasName(data)) {
*   data.name // => "Ada"
* }
* ```
*
* @see {@link isTagged}
* @see {@link isObjectKeyword}
* @stability stable
* @category guards
* @since 2.0.0
*/
const hasProperty = /*#__PURE__*/ dual(2, (self, property) => isObjectKeyword(self) && property in self);
/**
* Checks whether a value is an `Error`.
*
* **When to use**
*
* Use when you need a `Predicate` guard for errors caught from unknown sources.
*
* **Details**
*
* Uses `instanceof Error`.
*
* **Example** (Guarding errors)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = new Error("boom")
*
* Predicate.isError(data) // => true
* ```
*
* @see {@link isUnknown}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isError(input) {
	return input instanceof Error;
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Hash.js
/**
* Computes Effect hash values and defines the interface for objects that want
* to provide their own hash implementation. Hashes are small numeric
* fingerprints used by Effect data structures to bucket values quickly; they
* are not cryptographic digests and they are not proof that two values are
* equal. The module also includes helpers for primitive, structure, array, and
* reference-based hashes, plus functions for combining and optimizing numeric
* hash values.
*
* @stability stable
* @since 2.0.0
*/
/**
* Defines the unique identifier used to identify objects that implement the Hash interface.
*
* **When to use**
*
* Use as the computed property key for the method that supplies a custom hash
* value on a `Hash` implementor.
*
* @see {@link Hash} for the interface implemented with this symbol
* @see {@link isHash} for checking whether a value implements `Hash`
* @see {@link hash} for computing hash values
*
* @stability stable
* @category symbols
* @since 2.0.0
*/
const symbol$1 = "~effect/Hash";
/**
* Computes a hash value for any given value.
*
* **When to use**
*
* Use to compute an Effect hash for primitives, collections, and hashable
* objects.
*
* **Details**
*
* This function can hash primitives (numbers, strings, booleans, etc.) as well as
* objects, arrays, and other complex data structures. It automatically handles
* different types and provides a consistent hash value for equivalent inputs.
*
* **Gotchas**
*
* Objects being hashed must be treated as immutable after their first hash
* computation. Hash results are cached, so mutating an object after hashing will
* lead to stale cached values and broken hash-based operations. For mutable
* objects, implement a custom `Hash` interface that hashes the object reference
* rather than its content.
*
* **Example** (Hashing different values)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* Hash.hash(42) === Hash.hash(42) // => true
* Hash.hash("hello") === Hash.hash("hello") // => true
* Hash.hash([1, 2, 3]) === Hash.hash([1, 2, 3]) // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const hash = (self) => {
	switch (typeof self) {
		case "number": return number$4(self);
		case "bigint": return string$2(self.toString(10));
		case "string": return string$2(self);
		case "function":
		case "object": if (self === null) break;
		else if (self instanceof Date) {
			if (Number.isNaN(self.getTime())) return string$2("Invalid Date");
			return string$2(self.toISOString());
		} else if (self instanceof RegExp) return string$2(self.toString());
		else {
			if (byReferenceInstances.has(self)) return random$2(self);
			const cached = hashCache.get(self);
			if (cached !== void 0) return cached;
			if (visitedObjects.has(self)) {
				addBackEdge();
				return string$2("[Circular]");
			}
			visitedObjects.add(self);
			const seen = backEdges;
			let h;
			try {
				if (isHash(self)) h = self[symbol$1]();
				else if (typeof self === "function") h = random$2(self);
				else if (isArrayBufferLike(self)) {
					const bytes = bufferBytes(self);
					h = bytes === void 0 ? random$2(self) : array$1(bytes);
				} else if (self instanceof DataView) h = array$1(viewBytes(self));
				else if (Array.isArray(self) || ArrayBuffer.isView(self)) h = array$1(self);
				else if (self instanceof Map) h = hashMap(self);
				else if (self instanceof Set) h = hashSet(self);
				else h = structure(self);
			} finally {
				visitedObjects.delete(self);
			}
			if (seen === backEdges) hashCache.set(self, h);
			return h;
		}
	}
	return optimize(mix(string$2(String(self))));
};
/**
* Generates a random hash value for an object and caches it.
*
* **When to use**
*
* Use to hash an object by reference identity instead of structural content.
*
* **Details**
*
* This function creates a random hash value for objects that don't have their own
* hash implementation. The hash value is cached using a WeakMap, so the same object
* will always return the same hash value during its lifetime.
*
* **Example** (Hashing objects by reference)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* const obj1 = { a: 1 }
* const obj2 = { a: 1 }
*
* Hash.random(obj1) === Hash.random(obj1) // => true
*
* typeof Hash.random(obj2) // => "number"
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const random$2 = (self) => {
	if (!randomHashCache.has(self)) randomHashCache.set(self, optimize(Math.random() * 4294967296 | 0));
	return randomHashCache.get(self);
};
const mix = (h) => {
	h ^= h >>> 16;
	h = Math.imul(h, 2246822507);
	h ^= h >>> 13;
	h = Math.imul(h, 3266489909);
	return h ^ h >>> 16;
};
/**
* Combines two hash values into a single hash value.
*
* **When to use**
*
* Use to build a hash for a composite value by folding together hash values for
* its parts.
*
* **Details**
*
* Supports direct and pipeable usage. Argument order affects the result.
*
* **Example** (Combining hash values)
*
* ```ts import.meta.vitest
* import { Hash, pipe } from "effect"
*
* const hash1 = Hash.hash("hello")
* const hash2 = Hash.hash("world")
*
* const combined = Hash.combine(hash2)(hash1)
* combined === pipe(hash1, Hash.combine(hash2)) // => true
* ```
*
* @see {@link hash} for computing hash values from arbitrary inputs
* @see {@link structureKeys} for hashing selected object fields without manual combination
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const combine = /*#__PURE__*/ dual(2, (self, b) => mix(Math.imul(self, 2654435761) + Math.imul(b, 2246822507)));
/**
* Applies bit manipulation techniques to optimize a hash value.
*
* **When to use**
*
* Use to improve the bit distribution of a raw numeric hash value.
*
* **Details**
*
* This function takes a hash value and applies bitwise operations to improve
* the distribution of hash values, reducing the likelihood of collisions.
*
* **Example** (Optimizing a hash value)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* Hash.optimize(1234567890) // => 160826066
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const optimize = (n) => n & 3221225471 | n >>> 1 & 1073741824;
/**
* Checks whether a value implements the Hash interface.
*
* **When to use**
*
* Use to detect whether an unknown value provides a custom hash implementation.
*
* **Details**
*
* This function determines whether a given value has a function under the Hash
* symbol, indicating that it can provide its own hash value implementation.
* Data that merely carries the key, such as parsed JSON, is hashed as data.
*
* **Example** (Checking for Hash support)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* class MyHashable implements Hash.Hash {
*   [Hash.symbol]() {
*     return 42
*   }
* }
*
* Hash.isHash(new MyHashable()) // => true
* Hash.isHash({}) // => false
* Hash.isHash("string") // => false
* ```
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isHash = (u) => hasProperty(u, "~effect/Hash") && typeof u["~effect/Hash"] === "function";
const float64 = /*#__PURE__*/ new DataView(/*#__PURE__*/ new ArrayBuffer(8));
/**
* Computes a hash value for a number.
*
* **When to use**
*
* Use to hash a JavaScript number with Effect's numeric hash semantics.
*
* **Details**
*
* Int32 values hash to themselves. Other numbers hash from their IEEE-754 bits,
* with a canonical representation for `NaN`.
*
* **Example** (Hashing numbers)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* Number.isInteger(Hash.number(42)) // => true
* Number.isInteger(Hash.number(3.14)) // => true
* Hash.number(NaN) === Hash.number(NaN) // => true
* Hash.number(Infinity) === Hash.number(Infinity) // => true
* Hash.number(100) === Hash.number(100) // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const number$4 = (n) => {
	const h = n | 0;
	if (h === n) return optimize(h);
	float64.setFloat64(0, n !== n ? NaN : n);
	return optimize(combine(float64.getInt32(0), float64.getInt32(4)));
};
/**
* Computes a hash value for a string using the djb2 algorithm.
*
* **When to use**
*
* Use when you need a string field to contribute to a custom structural hash
* implementation.
*
* **Details**
*
* This function implements a variation of the djb2 hash algorithm, which is
* known for its good distribution properties and speed. It processes each
* character of the string to produce a consistent hash value.
*
* **Example** (Hashing strings)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* Hash.string("hello") // => 181380007
* Hash.string("world") // => 164394279
* Hash.string("") // => 5381
* Hash.string("test") === Hash.string("test") // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const string$2 = (str) => {
	let h = 5381, i = str.length;
	while (i) h = h * 33 ^ str.charCodeAt(--i);
	return optimize(h);
};
/**
* Computes a hash value for an object using only the specified keys.
*
* **When to use**
*
* Use to hash an object by a selected set of property keys.
*
* **Details**
*
* This function allows you to hash an object by considering only specific keys,
* which is useful when you want to create a hash based on a subset of an object's
* properties.
*
* **Example** (Hashing selected object keys)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* const person = { name: "John", age: 30, city: "New York" }
*
* const hash1 = Hash.structureKeys(person, ["name", "age"])
* const hash2 = Hash.structureKeys(person, ["name", "city"])
*
* hash1 // => -731887653
* hash2 // => 148523102
*
* const person2 = { name: "John", age: 30, city: "Boston" }
* const hash3 = Hash.structureKeys(person2, ["name", "age"])
* hash1 === hash3 // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const structureKeys = (o, keys) => {
	let h = 12289;
	for (const key of keys) h ^= combine(hash(key), hash(o[key]));
	return optimize(h);
};
/**
* Computes a structural hash for an object using Effect's object key collection.
*
* **When to use**
*
* Use to hash an object from all structural keys collected by Effect.
*
* **Details**
*
* The hash is based on the object's structural keys and their values, including
* symbol keys and relevant prototype keys for non-plain objects.
*
* **Example** (Hashing object structures)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* const obj1 = { name: "John", age: 30 }
* const obj2 = { name: "Jane", age: 25 }
* const obj3 = { name: "John", age: 30 }
*
* Hash.structure(obj1) // => -731887653
* Hash.structure(obj2) // => -222100417
* Hash.structure(obj3) // => -731887653
* Hash.structure(obj1) === Hash.structure(obj3) // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const structure = (o) => structureKeys(o, getAllObjectKeys(o));
const unordered = (seed, f) => (iter) => {
	let h = seed;
	for (const element of iter) h ^= f(element);
	return optimize(h);
};
/**
* Computes a hash value for an iterable by hashing all of its elements.
*
* **When to use**
*
* Use to hash the values yielded by an iterable with Effect hash semantics.
*
* **Details**
*
* Folds element hashes with {@link combine}, so order and length affect the
* result.
*
* **Gotchas**
*
* A hash is not an equality proof. Distinct inputs can still share a hash.
*
* **Example** (Hashing arrays)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* const arr1 = [1, 2, 3]
* const arr2 = [1, 2, 3]
* const arr3 = [3, 2, 1]
*
* Hash.array(arr1) === Hash.array(arr2) // => true
* Hash.array(arr1) === Hash.array(arr3) // => false
* ```
*
* @see {@link hash} for the general-purpose hash dispatcher
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const array$1 = (arr) => {
	let h = 6151;
	for (const element of arr) h = combine(h, hash(element));
	return optimize(h);
};
const hashMap = /*#__PURE__*/ unordered(/*#__PURE__*/ string$2("Map"), ([k, v]) => combine(hash(k), hash(v)));
const setSeed = /*#__PURE__*/ string$2("Set");
const hashSet = /*#__PURE__*/ unordered(setSeed, (element) => combine(setSeed, hash(element)));
const randomHashCache = /*#__PURE__*/ new WeakMap();
const hashCache = /*#__PURE__*/ new WeakMap();
const visitedObjects = /*#__PURE__*/ new WeakSet();
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Equal.js
/**
* Defines the unique string identifier for the `Equal` interface.
*
* **When to use**
*
* Use when you implement custom equality and need the computed property key for
* the equality method.
*
* **Details**
*
* This is a pure constant with no allocation or side effects.
*
* **Example** (Implementing Equal on a class)
*
* ```ts import.meta.vitest
* import { Equal, Hash } from "effect"
*
* class UserId implements Equal.Equal {
*   constructor(readonly id: string) {}
*
*   [Equal.symbol](that: Equal.Equal): boolean {
*     return that instanceof UserId && this.id === that.id
*   }
*
*   [Hash.symbol](): number {
*     return Hash.string(this.id)
*   }
* }
*
* Equal.equals(new UserId("1"), new UserId("1")) // => true
* Equal.equals(new UserId("1"), new UserId("2")) // => false
* ```
*
* @see {@link Equal} — the interface that uses this symbol
* @see {@link isEqual} — type guard for `Equal` implementors
* @stability stable
* @category symbols
* @since 2.0.0
*/
const symbol = "~effect/Equal";
function equals$1() {
	if (arguments.length === 1) return (self) => compareBoth(self, arguments[0]);
	return compareBoth(arguments[0], arguments[1]);
}
function compareBoth(self, that) {
	if (self === that) return true;
	if (self == null || that == null) return false;
	const selfType = typeof self;
	if (selfType !== typeof that) return false;
	if (selfType === "number" && self !== self && that !== that) return true;
	if (selfType !== "object" && selfType !== "function") return false;
	if (byReferenceInstances.has(self) || byReferenceInstances.has(that)) return false;
	return compareObjects(self, that);
}
function compareObjects(self, that) {
	const depth = pathLeft.length;
	for (let i = depth; i-- > 0;) if (pathLeft[i] === self && pathRight[i] === that) return true;
	if (depth) return compareOnPath(self, that);
	let known = results.get(self);
	if (!known) results.set(self, known = /* @__PURE__ */ new WeakMap());
	let result = known.get(that);
	if (result === void 0) known.set(that, result = compareOnPath(self, that));
	return result;
}
function compareOnPath(self, that) {
	pathLeft.push(self);
	pathRight.push(that);
	try {
		return compareStructure(self, that);
	} finally {
		pathLeft.pop();
		pathRight.pop();
	}
}
const pathLeft = [];
const pathRight = [];
const results = /*#__PURE__*/ new WeakMap();
function compareStructure(self, that) {
	if (hash(self) !== hash(that)) return false;
	else if (self instanceof Date) {
		if (!(that instanceof Date)) return false;
		const selfTime = self.getTime();
		const thatTime = that.getTime();
		return selfTime === thatTime || Number.isNaN(selfTime) && Number.isNaN(thatTime);
	} else if (self instanceof RegExp) return that instanceof RegExp && self.toString() === that.toString();
	const bothEquals = isEqual(self);
	if (bothEquals !== isEqual(that) || typeof self === "function" && !bothEquals) return false;
	else if (bothEquals) return self[symbol](that);
	else if (Array.isArray(self)) {
		if (!Array.isArray(that) || self.length !== that.length) return false;
		return compareArrays(self, that);
	} else if (isArrayBufferLike(self)) {
		if (!isArrayBufferLike(that) || self instanceof ArrayBuffer !== that instanceof ArrayBuffer) return false;
		const selfBytes = bufferBytes(self);
		const thatBytes = bufferBytes(that);
		return selfBytes !== void 0 && thatBytes !== void 0 && compareTypedArrays(selfBytes, thatBytes);
	} else if (ArrayBuffer.isView(self)) {
		const selfIsDataView = self instanceof DataView;
		if (!ArrayBuffer.isView(that) || self.byteLength !== that.byteLength || selfIsDataView !== that instanceof DataView) return false;
		if (selfIsDataView) return compareTypedArrays(viewBytes(self), viewBytes(that));
		return compareTypedArrays(self, that);
	} else if (self instanceof Map) {
		if (!(that instanceof Map) || self.size !== that.size) return false;
		return compareHashed(self, that, entryHash, equalEntries);
	} else if (self instanceof Set) {
		if (!(that instanceof Set) || self.size !== that.size) return false;
		return compareHashed(self, that, hash, compareBoth);
	}
	return compareRecords(self, that);
}
function compareArrays(self, that) {
	for (let i = 0; i < self.length; i++) if (!compareBoth(self[i], that[i])) return false;
	return true;
}
function compareTypedArrays(self, that) {
	if (self.length !== that.length) return false;
	for (let i = 0; i < self.length; i++) if (self[i] !== that[i]) return false;
	return true;
}
function compareRecords(self, that) {
	const selfKeys = getAllObjectKeys(self);
	const thatKeys = getAllObjectKeys(that);
	if (selfKeys.size !== thatKeys.size) return false;
	for (const key of selfKeys) if (!thatKeys.has(key) || !compareBoth(self[key], that[key])) return false;
	return true;
}
function compareHashed(self, that, hashOf, equivalent) {
	const groups = /* @__PURE__ */ new Map();
	let remaining = 0;
	for (const item of that) {
		const h = hashOf(item);
		const group = groups.get(h);
		if (group) group.push(item);
		else groups.set(h, [item]);
		remaining++;
	}
	outer: for (const item of self) {
		const group = groups.get(hashOf(item));
		if (group) {
			for (let i = 0; i < group.length; i++) if (equivalent(item, group[i])) {
				group[i] = group[group.length - 1];
				group.pop();
				remaining--;
				continue outer;
			}
		}
		return false;
	}
	return remaining === 0;
}
const entryHash = (entry) => hash(entry[0]);
const equalEntries = (self, that) => compareBoth(self[0], that[0]) && compareBoth(self[1], that[1]);
/**
* Checks whether a value implements the {@link Equal} interface.
*
* **When to use**
*
* Use when you need generic utility code to distinguish `Equal` implementors
* from plain values before calling `[Equal.symbol]` directly.
*
* **Details**
*
* - Pure function, no side effects.
* - Returns `true` if and only if `u` has a function keyed by {@link symbol}.
*   Data that merely carries the key, such as parsed JSON, is not `Equal`.
* - Acts as a TypeScript type guard, narrowing the input to {@link Equal}.
*
* **Example** (Checking Equal values)
*
* ```ts import.meta.vitest
* import { Equal, Hash } from "effect"
*
* class Token implements Equal.Equal {
*   constructor(readonly value: string) {}
*   [Equal.symbol](that: Equal.Equal): boolean {
*     return that instanceof Token && this.value === that.value
*   }
*   [Hash.symbol](): number {
*     return Hash.string(this.value)
*   }
* }
*
* Equal.isEqual(new Token("abc")) // => true
* Equal.isEqual({ x: 1 }) // => false
* Equal.isEqual(42) // => false
* ```
*
* @see {@link Equal} — the interface being checked
* @see {@link symbol} — the property key that signals `Equal` support
* @stability stable
* @category guards
* @since 2.0.0
*/
const isEqual = (u) => hasProperty(u, "~effect/Equal") && typeof u["~effect/Equal"] === "function";
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/array.js
/**
* @since 2.0.0
*/
/** @internal */
const isArrayNonEmpty$1 = (self) => self.length > 0;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/count.js
/**
* Normalizes a collection count to a non-negative integer. `NaN` and
* non-positive values become `0`; positive infinity is preserved.
*
* @internal
*/
const normalize$1 = (n) => n > 0 ? Math.floor(n) : 0;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/record.js
/** @internal */
function assignProperty(self, key, value) {
	if (key === "__proto__") Object.defineProperty(self, key, {
		value,
		writable: true,
		enumerable: true,
		configurable: true
	});
	else self[key] = value;
}
/** @internal */
function assignProperties(self, source) {
	if (!Object.hasOwn(source, "__proto__")) {
		Object.assign(self, source);
		return;
	}
	for (const key of Reflect.ownKeys(source)) if (Object.prototype.propertyIsEnumerable.call(source, key)) assignProperty(self, key, source[key]);
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Redactable.js
/**
* Defines the symbol used to identify objects that implement the {@link Redactable}
* protocol.
*
* **When to use**
*
* Use as the property key when implementing the `Redactable` protocol.
*
* **Details**
*
* Add a method under this key to make an object redactable. The method receives
* the current `Context` and must return the replacement value. The symbol is
* registered globally via `Symbol.for("~effect/Redactable")`, so it is
* identical across multiple copies of the library at runtime.
*
* **Example** (Masking an API key)
*
* ```ts import.meta.vitest
* import { Context, Redactable } from "effect"
*
* class ApiKey {
*   constructor(readonly raw: string) {}
*
*   [Redactable.symbolRedactable](_ctx: Context.Context<never>) {
*     return this.raw.slice(0, 4) + "..."
*   }
* }
*
* Redactable.redact(new ApiKey("secret-key")) // => "secr..."
* ```
*
* @see {@link Redactable} for the interface this symbol belongs to
* @see {@link isRedactable} to check whether a value has this symbol
* @stability stable
* @category symbols
* @since 3.10.0
*/
const symbolRedactable = /*#__PURE__*/ Symbol.for("~effect/Redactable");
/**
* Type guard that checks whether a value implements the {@link Redactable}
* interface.
*
* **When to use**
*
* Use to narrow an unknown value before calling redaction-specific helpers.
*
* @see {@link Redactable} for the interface being checked
* @see {@link redact} to apply redaction if the value is redactable
* @stability stable
* @category guards
* @since 3.10.0
*/
const isRedactable = (u) => hasProperty(u, symbolRedactable);
/**
* Returns a redacted value if it implements {@link Redactable}, otherwise returns it
* unchanged.
*
* **When to use**
*
* Use as the general-purpose entry point for redaction when the input may
* or may not implement the redaction protocol.
*
* **Details**
*
* This function calls {@link isRedactable} and, when it returns `true`,
* delegates to {@link getRedacted}.
*
* **Gotchas**
*
* Redaction is not recursive. Nested redactable values inside the returned
* object are not automatically redacted.
*
* @see {@link isRedactable} to check before redacting
* @see {@link getRedacted} for the lower-level variant for known redactables
* @stability stable
* @category destructors
* @since 3.10.0
*/
function redact$2(u) {
	if (isRedactable(u)) return getRedacted(u);
	return u;
}
/**
* Returns the result of calling `[symbolRedactable]` on a value that is
* already known to be {@link Redactable}.
*
* **When to use**
*
* Use when you need to read the redacted representation from a value already
* verified as `Redactable`.
*
* **Details**
*
* This function reads the current fiber's `Context` from the global fiber
* reference and passes it to the redaction method.
*
* **Gotchas**
*
* If no fiber is active, an empty `Context` is passed to the redaction method.
*
* @see {@link redact} for the higher-level variant that handles non-redactable values
* @see {@link isRedactable} for the type guard to verify before calling this
* @stability stable
* @category destructors
* @since 4.0.0
*/
function getRedacted(redactable) {
	return redactable[symbolRedactable](globalThis["~effect/Fiber/currentFiber"]?.context ?? emptyContext$1);
}
/** @internal */
const currentFiberTypeId = "~effect/Fiber/currentFiber";
const emptyMap = /*#__PURE__*/ new Map();
const emptyContext$1 = {
	"~effect/Context": {},
	base: emptyMap,
	depth: 0,
	mapUnsafe: emptyMap,
	pipe() {
		return pipeArguments(this, arguments);
	}
};
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Formatter.js
/**
* Formats JavaScript values into readable strings.
*
* `format` is intended for logs, diagnostics, and error messages. It handles
* primitives, objects, arrays, dates, regular expressions, maps, sets, class
* instances, errors, circular references, and redactable values. `formatJson`
* wraps JSON formatting with redaction and circular-reference handling, and the
* module also includes helpers for property keys, paths, and dates.
*
* @stability stable
* @since 4.0.0
*/
/**
* Converts any JavaScript value into a human-readable string.
*
* **When to use**
*
* Use when you need to format arbitrary JavaScript values for debugging,
* logging, or error messages.
*
* **Details**
*
* - Output is **not** valid JSON; use {@link formatJson} when you need
*   parseable JSON.
* - Handles `BigInt`, `Symbol`, `Set`, `Map`, `Date`, `RegExp`, and class
*   instances that `JSON.stringify` cannot represent.
* - Circular references are shown as `"[Circular]"` instead of throwing.
* - Failures while inspecting a value are rendered as diagnostic placeholders instead of throwing.
* - Primitives: stringified naturally (`null`, `undefined`, `123`, `true`).
*   Strings are JSON-quoted.
* - Objects with a custom `toString` (not `Object.prototype.toString`):
*   `toString()` is called unless `ignoreToString` is `true`.
* - Errors with a `cause`: formatted as `"<message> (cause: <cause>)"`.
* - Iterables (`Set`, `Map`, etc.): formatted as
*   `ClassName([...elements])`.
* - Class instances: wrapped as `ClassName({...})`.
* - `Redactable` values are automatically redacted.
* - Arrays/objects with 0–1 entries are inline; larger ones are
*   pretty-printed when `space` is set.
* - `space` — indentation unit (number of spaces, or a string like
*   `"\t"`). Defaults to `0` (compact).
* - `ignoreToString` — skip calling `toString()`. Defaults to `false`.
*
* **Example** (Formatting compact output)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* Formatter.format({ a: 1, b: [2, 3] }) // => "{\"a\":1,\"b\":[2,3]}"
* ```
*
* **Example** (Pretty-printed output)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* const output = Formatter.format({ a: 1, b: [2, 3] }, { space: 2 })
* output // => "{\n  \"a\": 1,\n  \"b\": [\n    2,\n    3\n  ]\n}"
* ```
*
* **Example** (Handling circular references)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* const obj: any = { name: "loop" }
* obj.self = obj
* Formatter.format(obj) // => "{\"name\":\"loop\",\"self\":[Circular]}"
* ```
*
* @see {@link formatJson}
* @see {@link Formatter}
* @stability stable
* @category formatting
* @since 2.0.0
*/
function format$1(input, options) {
	const space = options?.space ?? 0;
	const ancestors = /* @__PURE__ */ new WeakSet();
	const gap = !space ? "" : typeof space === "number" ? " ".repeat(space) : space;
	const ind = (d) => gap.repeat(d);
	const wrap = (v, body) => {
		const ctor = v?.constructor;
		return ctor && ctor !== Object.prototype.constructor && ctor.name ? `${ctor.name}(${body})` : body;
	};
	const ownKeys = (o) => {
		try {
			return Reflect.ownKeys(o);
		} catch {
			return ["[ownKeys threw]"];
		}
	};
	function recur(v, d = 0) {
		try {
			return recurUnsafe(v, d);
		} catch {
			if (typeof v === "object" && v !== null || typeof v === "function") ancestors.delete(v);
			return "[inspection threw]";
		}
	}
	function recurUnsafe(v, d = 0) {
		if (typeof v === "string") return JSON.stringify(v);
		if (typeof v === "number" || v == null || typeof v === "boolean" || typeof v === "symbol") return String(v);
		if (typeof v === "bigint") return String(v) + "n";
		if (typeof v === "object" || typeof v === "function") {
			if (ancestors.has(v)) return CIRCULAR;
			ancestors.add(v);
			let output;
			if (symbolRedactable in v) output = recur(getRedacted(v), d);
			else if (Array.isArray(v)) output = !gap || v.length <= 1 ? `[${v.map((x) => recur(x, d)).join(",")}]` : `[\n${ind(d + 1)}${v.map((x) => recur(x, d + 1)).join(",\n" + ind(d + 1))}\n${ind(d)}]`;
			else if (v instanceof Date) output = formatDate(v);
			else if (!options?.ignoreToString && hasProperty(v, "toString") && typeof v["toString"] === "function" && v["toString"] !== Object.prototype.toString && v["toString"] !== Array.prototype.toString) {
				const s = safeToString(v);
				output = v instanceof Error && v.cause !== void 0 ? `${s} (cause: ${recur(v.cause, d)})` : s;
			} else if (Symbol.iterator in v) output = `${v.constructor.name}(${recur(Array.from(v), d)})`;
			else {
				const keys = ownKeys(v);
				if (!gap || keys.length <= 1) {
					const body = `{${keys.map((k) => `${formatPropertyKey(k)}:${recur(safeGet(v, k), d)}`).join(",")}}`;
					output = wrap(v, body);
				} else {
					const body = `{\n${keys.map((k) => `${ind(d + 1)}${formatPropertyKey(k)}: ${recur(safeGet(v, k), d + 1)}`).join(",\n")}\n${ind(d)}}`;
					output = wrap(v, body);
				}
			}
			ancestors.delete(v);
			return output;
		}
		return String(v);
	}
	return recur(input, 0);
}
const CIRCULAR = "[Circular]";
/**
* @internal
*/
function formatPropertyKey(name) {
	return typeof name === "string" ? JSON.stringify(name) : String(name);
}
/**
* Formats a `Date` as an ISO 8601 string, returning `"Invalid Date"` for
* invalid dates instead of throwing.
*
* @internal
*/
function formatDate(date) {
	try {
		return date.toISOString();
	} catch {
		return "Invalid Date";
	}
}
function safeToString(input) {
	try {
		const s = input.toString();
		return typeof s === "string" ? s : String(s);
	} catch {
		return "[toString threw]";
	}
}
function safeGet(input, key) {
	try {
		return input[key];
	} catch {
		return "[property access threw]";
	}
}
/**
* Stringifies a value to JSON safely, silently dropping circular references.
*
* **When to use**
*
* Use when you need valid JSON output, unlike `format`, and the input may
* contain circular references that should be silently omitted rather than
* throwing a `TypeError`.
*
* **Details**
*
* Uses `JSON.stringify` internally with a replacer that tracks the current
* object ancestry. Circular references are replaced with `undefined`, which
* omits them from object output. `Redactable` values are automatically redacted
* before serialization. `BigInt` values are stringified with an `n` suffix.
* `Error` instances without a `toJSON` property include their enumerable
* properties plus `name` and `message`. Errors with `toJSON` keep their custom
* representation. Other values follow standard `JSON.stringify` behavior. The
* `space` parameter controls indentation and defaults to `0`.
*
* **Gotchas**
*
* When the root input is `undefined`, a symbol, or a function, `formatJson`
* returns `"null"` instead of the `undefined` returned by `JSON.stringify`.
* Nested values retain standard `JSON.stringify` behavior.
*
* **Example** (Formatting compact JSON)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* Formatter.formatJson({ name: "Alice", age: 30 }) // => "{\"name\":\"Alice\",\"age\":30}"
* ```
*
* **Example** (Handling circular references)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* const obj: any = { name: "test" }
* obj.self = obj
* Formatter.formatJson(obj) // => "{\"name\":\"test\"}"
* ```
*
* **Example** (Pretty-printed JSON)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* const output = Formatter.formatJson({ name: "Alice", age: 30 }, { space: 2 })
* output // => "{\n  \"name\": \"Alice\",\n  \"age\": 30\n}"
* ```
*
* @see {@link format}
* @see {@link Formatter}
* @stability stable
* @category serialization
* @since 4.0.0
*/
function formatJson(input, options) {
	const ancestors = [];
	const sources = /* @__PURE__ */ new WeakMap();
	return JSON.stringify(input, function(key, value) {
		const original = Object.getOwnPropertyDescriptor(sources.get(this) ?? this, key)?.value;
		const redacted = hasProperty(original, symbolRedactable) ? redact$2(original) : redact$2(value);
		if (typeof redacted === "bigint") return format$1(redacted);
		if (typeof redacted !== "object" || redacted === null) return redacted;
		const current = redacted instanceof Error && !hasProperty(redacted, "toJSON") ? {
			...redacted,
			name: redacted.name,
			message: redacted.message
		} : redacted;
		while (ancestors.length > 0 && ancestors[ancestors.length - 1] !== this) ancestors.pop();
		if (ancestors.includes(redacted)) return;
		ancestors.push(redacted);
		if (current !== redacted) ancestors.push(current);
		if (!hasGetter(current) || isJsonPrimitiveWrapper(current)) return current;
		const view = redactingView(current);
		sources.set(view, current);
		ancestors.push(view);
		return view;
	}, options?.space) ?? "null";
}
function hasGetter(object) {
	for (const key of Object.getOwnPropertyNames(object)) if (Object.getOwnPropertyDescriptor(object, key)?.get !== void 0) return true;
	return false;
}
function redactingView(object) {
	const view = Array.isArray(object) ? [] : {};
	const keys = Array.isArray(object) ? Array.from({ length: object.length }, (_, i) => String(i)) : Object.keys(object);
	for (const key of keys) Object.defineProperty(view, key, {
		enumerable: true,
		get: () => {
			const value = Reflect.get(object, key);
			return Object.getOwnPropertyDescriptor(object, key)?.get !== void 0 ? redact$2(value) : value;
		}
	});
	return view;
}
function isJsonPrimitiveWrapper(object) {
	for (const valueOf of [
		Number.prototype.valueOf,
		Boolean.prototype.valueOf,
		String.prototype.valueOf
	]) try {
		Reflect.apply(valueOf, object, []);
		return true;
	} catch {}
	return false;
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Inspectable.js
/**
* Controls how values appear in logs and debugging output.
*
* Effect data types use `Inspectable` to provide stable string, JSON, and
* Node.js inspection output. This keeps custom values readable in logs, REPLs,
* test failures, and diagnostics. This module defines the Node inspect symbol,
* the `Inspectable` interface, safe conversion helpers, and shared prototype or
* class implementations for custom values.
*
* @stability stable
* @since 2.0.0
*/
/**
* Defines the symbol used by Node.js for custom object inspection.
*
* **When to use**
*
* Use to implement Node.js custom inspection for a value.
*
* **Details**
*
* This symbol is recognized by Node.js's `util.inspect()` function and the REPL
* for custom object representation. When an object has a method with this symbol,
* it will be called to determine how the object should be displayed.
*
* **Example** (Defining custom Node inspection)
*
* ```ts import.meta.vitest
* import { Inspectable } from "effect"
*
* class CustomObject {
*   constructor(private value: string) {}
*
*   [Inspectable.NodeInspectSymbol]() {
*     return `CustomObject(${this.value})`
*   }
* }
*
* const obj = new CustomObject("hello")
* obj[Inspectable.NodeInspectSymbol]() // => "CustomObject(hello)"
* ```
*
* @stability stable
* @category symbols
* @since 2.0.0
*/
const NodeInspectSymbol = /*#__PURE__*/ Symbol.for("nodejs.util.inspect.custom");
/**
* Converts a value to its structured inspection representation.
*
* **When to use**
*
* Use when you need the structured representation of an inspectable value
* without risking unhandled errors.
*
* **Details**
*
* This function applies redaction before extracting data from objects that
* implement `toJSON`, recursively processes arrays, and handles errors
* gracefully. Plain objects are returned unchanged, so the result is not
* guaranteed to be accepted by `JSON.stringify`; it may still contain values
* such as `BigInt`, functions, or circular references.
*
* @see {@link toStringUnknown} for converting unknown values to strings
*
* @stability stable
* @category converting
* @since 4.0.0
*/
const toJson = (input) => {
	try {
		input = redact$2(input);
		if (hasProperty(input, "toJSON") && isFunction(input["toJSON"]) && input["toJSON"].length === 0) return input.toJSON();
		else if (Array.isArray(input)) return input.map(toJson);
		return input;
	} catch {
		return "[toJSON threw]";
	}
};
/**
* A base prototype object that implements the {@link Inspectable} interface.
*
* **When to use**
*
* Use as a prototype for plain objects that should share standard inspectable behavior.
*
* **Details**
*
* This object provides default implementations for the {@link Inspectable} methods.
* It can be used as a prototype for objects that want to be inspectable,
* or as a mixin to add inspection capabilities to existing objects.
*
* **Example** (Using the base inspectable prototype)
*
* ```ts import.meta.vitest
* import { Inspectable } from "effect"
*
* // Use as prototype
* const myObject = Object.create(Inspectable.BaseProto)
* myObject.name = "example"
* myObject.value = 42
*
* myObject.toString() // => "\"[toJSON threw]\""
*
* // Or extend in a constructor
* function MyClass(this: any, name: string) {
*   this.name = name
* }
* MyClass.prototype = Object.create(Inspectable.BaseProto)
* MyClass.prototype.constructor = MyClass
* ```
*
* @stability stable
* @category prototypes
* @since 2.0.0
*/
const BaseProto = {
	toJSON() {
		return toJson(this);
	},
	[NodeInspectSymbol]() {
		return this.toJSON();
	},
	toString() {
		return format$1(this.toJSON());
	}
};
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/stackTraceLimit.js
/**
* Check if `Error.stackTraceLimit` is writable.
* Returns `false` if the property is frozen, non-writable, or `Error` is non-extensible.
*
* @internal
*/
const isStackTraceLimitWritable = () => {
	const desc = Object.getOwnPropertyDescriptor(Error, "stackTraceLimit");
	if (desc === void 0) return Object.isExtensible(Error);
	return Object.hasOwn(desc, "writable") ? desc.writable === true : desc.set !== void 0;
};
const canWriteStackTraceLimit = /*#__PURE__*/ isStackTraceLimitWritable();
/**
* Get the current `Error.stackTraceLimit` value.
* Returns `undefined` if the property doesn't exist.
*
* @internal
*/
const getStackTraceLimit = () => Error.stackTraceLimit;
/**
* Safely set `Error.stackTraceLimit` if possible, otherwise no-op.
*
* Accepts `undefined` so a value read via {@link getStackTraceLimit} can be
* restored faithfully.
*
* @internal
*/
const setStackTraceLimit = (value) => {
	if (canWriteStackTraceLimit) Error.stackTraceLimit = value;
};
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Utils.js
/**
* Yields its wrapped value exactly once, then completes with the value sent
* back in.
*
* **When to use**
*
* Use to implement `[Symbol.iterator]()` on Effect-like types so they can be
* `yield*`-ed inside generator functions, such as `Effect.gen` and
* `Option.gen`.
*
* **Details**
*
* The first call to `next()` returns a fresh `{ value: self, done: false }`.
* Every subsequent call returns `{ value: a, done: true }` where `a` is the
* argument passed to `next()`. To keep `yield*` cheap, the completion result
* is the iterator itself rather than a new object, so only the iterator and
* the single yielded result are allocated per `yield*`.
*
* **Example** (Yielding a wrapped value in a generator)
*
* ```ts import.meta.vitest
* import { Utils } from "effect"
*
* const gen = new Utils.SingleShotGen<string, number>("hello")
*
* gen.next(0).value // => "hello"
*
* gen.next(42).value // => 42
* ```
*
* @see {@link Gen} for the type-level signature that relies on `SingleShotGen`
* @stability stable
* @category constructors
* @since 2.0.0
*/
var SingleShotGen = class {
	constructor(self) {
		this.value = self;
		this.done = false;
	}
	/**
	* Yields the stored value once, then completes with the value sent back in.
	*
	* **When to use**
	*
	* Use to advance a `SingleShotGen` through its single yield and completion
	* step.
	*
	* @since 2.0.0
	*/
	next(a) {
		if (this.done) {
			this.value = a;
			return this;
		}
		this.done = true;
		return {
			value: this.value,
			done: false
		};
	}
};
const pickInternalCall = () => {
	const InternalTypeId = "~effect/Utils/internal";
	const standard = { [InternalTypeId]: (body) => {
		return body();
	} };
	const forced = { [InternalTypeId]: (body) => {
		try {
			return body();
		} finally {}
	} };
	const limit = getStackTraceLimit();
	if (limit === 0) return forced[InternalTypeId];
	setStackTraceLimit(2);
	const error = standard[InternalTypeId](() => /* @__PURE__ */ new Error());
	setStackTraceLimit(limit);
	return error.stack?.includes(InternalTypeId) === true ? standard[InternalTypeId] : forced[InternalTypeId];
};
/** @internal */
const internalCall = /*#__PURE__*/ pickInternalCall();
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/core.js
/** @internal */
const EffectTypeId = `~effect/Effect`;
/** @internal */
const ExitTypeId = `~effect/Exit`;
const effectVariance = {
	_A: identity$1,
	_E: identity$1,
	_R: identity$1
};
/** @internal */
const identifier = `${EffectTypeId}/identifier`;
/** @internal */
const args = `${EffectTypeId}/args`;
/** @internal */
const evaluate = `${EffectTypeId}/evaluate`;
/** @internal */
const contA = `${EffectTypeId}/successCont`;
/** @internal */
const contE = `${EffectTypeId}/failureCont`;
/** @internal */
const contAll = `${EffectTypeId}/ensureCont`;
/** @internal */
const Yield = /*#__PURE__*/ Symbol.for("effect/Effect/Yield");
/** @internal */
const PipeInspectableProto = {
	pipe() {
		return pipeArguments(this, arguments);
	},
	toJSON() {
		return { ...this };
	},
	toString() {
		return format$1(this.toJSON(), {
			ignoreToString: true,
			space: 2
		});
	},
	[NodeInspectSymbol]() {
		return this.toJSON();
	}
};
/** @internal */
const EffectProto = {
	[EffectTypeId]: effectVariance,
	...PipeInspectableProto,
	[Symbol.iterator]() {
		return new SingleShotGen(this);
	},
	toJSON() {
		return {
			_id: "Effect",
			op: this[identifier],
			..."~effect/Effect/args" in this ? { args: this[args] } : void 0
		};
	}
};
/** @internal */
const isEffect = (u) => hasProperty(u, EffectTypeId);
/** @internal */
const isExit = (u) => hasProperty(u, ExitTypeId);
/** @internal */
const CauseTypeId = "~effect/Cause";
/** @internal */
const CauseReasonTypeId = "~effect/Cause/Reason";
/** @internal */
const isCause = (self) => hasProperty(self, CauseTypeId);
/** @internal */
var CauseImpl = class {
	constructor(failures) {
		this[CauseTypeId] = CauseTypeId;
		this.reasons = failures;
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
	toJSON() {
		return {
			_id: "Cause",
			reasons: this.reasons.map((f) => f.toJSON())
		};
	}
	toString() {
		return `Cause(${format$1(this.reasons)})`;
	}
	[NodeInspectSymbol]() {
		return this.toJSON();
	}
	[symbol](that) {
		return isCause(that) && this.reasons.length === that.reasons.length && this.reasons.every((e, i) => equals$1(e, that.reasons[i]));
	}
	[symbol$1]() {
		return array$1(this.reasons);
	}
};
const annotationsMap = /*#__PURE__*/ new WeakMap();
/** @internal */
var ReasonBase = class {
	[CauseReasonTypeId];
	annotations;
	_tag;
	constructor(_tag, annotations, originalError) {
		this[CauseReasonTypeId] = CauseReasonTypeId;
		this._tag = _tag;
		if (annotations !== constEmptyAnnotations && typeof originalError === "object" && originalError !== null && annotations.size > 0) {
			const prevAnnotations = annotationsMap.get(originalError);
			if (prevAnnotations) annotations = new Map([...prevAnnotations, ...annotations]);
			annotationsMap.set(originalError, annotations);
		}
		this.annotations = annotations;
	}
	annotate(annotations, options) {
		if (annotations.mapUnsafe.size === 0) return this;
		const newAnnotations = new Map(this.annotations);
		annotations.mapUnsafe.forEach((value, key) => {
			if (options?.overwrite !== true && newAnnotations.has(key)) return;
			newAnnotations.set(key, value);
		});
		if (options?.overwrite !== true && newAnnotations.size === this.annotations.size) return this;
		const self = Object.assign(Object.create(Object.getPrototypeOf(this)), this);
		self.annotations = newAnnotations;
		return self;
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
	toString() {
		return format$1(this);
	}
	[NodeInspectSymbol]() {
		return this.toString();
	}
};
/** @internal */
const constEmptyAnnotations = /*#__PURE__*/ new Map();
/** @internal */
var Fail = class extends ReasonBase {
	constructor(error, annotations = constEmptyAnnotations) {
		super("Fail", annotations, error);
		this.error = error;
	}
	toString() {
		return `Fail(${format$1(this.error)})`;
	}
	toJSON() {
		return {
			_tag: "Fail",
			error: this.error
		};
	}
	[symbol](that) {
		return isFailReason$1(that) && equals$1(this.error, that.error) && equals$1(this.annotations, that.annotations);
	}
	[symbol$1]() {
		return combine(string$2(this._tag))(combine(hash(this.error))(hash(this.annotations)));
	}
};
/** @internal */
const causeFromReasons = (reasons) => new CauseImpl(reasons);
const dedupeReasons = (self, that) => {
	const buckets = /* @__PURE__ */ new Map();
	const out = [];
	for (const reason of self.concat(that)) {
		const hash$2 = hash(reason);
		const bucket = buckets.get(hash$2);
		if (bucket === void 0) buckets.set(hash$2, [reason]);
		else if (bucket.some((previous) => equals$1(previous, reason))) continue;
		else bucket.push(reason);
		out.push(reason);
	}
	return out;
};
/** @internal */
const causeCombine = /*#__PURE__*/ dual(2, (self, that) => {
	if (self.reasons.length === 0) return that;
	else if (that.reasons.length === 0) return self;
	const newCause = new CauseImpl(dedupeReasons(self.reasons, that.reasons));
	return equals$1(self, newCause) ? self : newCause;
});
/**
* Appends the interrupts from `interrupts` that `self` does not contain yet.
* Unlike `causeCombine`, it never calls hash or equality methods, which can be
* user-defined, random, or throwing.
*
* @internal
*/
const causeAddInterrupts = (self, interrupts) => {
	const reasons = self.reasons.slice();
	for (const reason of interrupts.reasons) if (isInterruptReason(reason) && !reasons.some((r) => isInterruptReason(r) && r.fiberId === reason.fiberId && r.annotations === reason.annotations)) reasons.push(reason);
	return reasons.length === self.reasons.length ? self : new CauseImpl(reasons);
};
/** @internal */
const causeFail = (error) => new CauseImpl([new Fail(error)]);
/** @internal */
var Die = class extends ReasonBase {
	constructor(defect, annotations = constEmptyAnnotations) {
		super("Die", annotations, defect);
		this.defect = defect;
	}
	toString() {
		return `Die(${format$1(this.defect)})`;
	}
	toJSON() {
		return {
			_tag: "Die",
			defect: this.defect
		};
	}
	[symbol](that) {
		return isDieReason(that) && equals$1(this.defect, that.defect) && equals$1(this.annotations, that.annotations);
	}
	[symbol$1]() {
		return combine(string$2(this._tag))(combine(hash(this.defect))(hash(this.annotations)));
	}
};
/** @internal */
const causeDie = (defect) => new CauseImpl([new Die(defect)]);
/** @internal */
const causeAnnotate = /*#__PURE__*/ dual((args) => isCause(args[0]), (self, annotations, options) => {
	if (annotations.mapUnsafe.size === 0) return self;
	return new CauseImpl(self.reasons.map((f) => f.annotate(annotations, options)));
});
/** @internal */
const isFailReason$1 = (self) => self._tag === "Fail";
/** @internal */
const isDieReason = (self) => self._tag === "Die";
/** @internal */
const isInterruptReason = (self) => self._tag === "Interrupt";
function defaultEvaluate(_fiber) {
	return exitDie(`Effect.evaluate: Not implemented`);
}
/** @internal */
const makePrimitiveProto = (options) => ({
	...EffectProto,
	[identifier]: options.op,
	[evaluate]: options["~effect/Effect/evaluate"] ?? defaultEvaluate,
	[contA]: options[contA],
	[contE]: options[contE],
	[contAll]: options[contAll]
});
/** @internal */
const makePrimitive = (options) => {
	const Proto = makePrimitiveProto(options);
	const PrimitiveImpl = function(value) {
		this[args] = value;
	};
	PrimitiveImpl.prototype = Proto;
	return function(value) {
		return new PrimitiveImpl(value);
	};
};
/** @internal */
const makeExit = (options) => {
	const Proto = {
		[ExitTypeId]: ExitTypeId,
		_tag: options.op,
		get [options.prop]() {
			return this[args];
		},
		...makePrimitiveProto(options),
		toString() {
			return `${options.op}(${format$1(this[args])})`;
		},
		toJSON() {
			return {
				_id: "Exit",
				_tag: options.op,
				[options.prop]: this[args]
			};
		},
		[symbol](that) {
			return isExit(that) && that._tag === this._tag && equals$1(this["~effect/Effect/args"], that["~effect/Effect/args"]);
		},
		[symbol$1]() {
			return combine(string$2(options.op), hash(this[args]));
		}
	};
	const ExitPrimitive = function(value) {
		this[args] = value;
	};
	ExitPrimitive.prototype = Proto;
	return function(value) {
		return new ExitPrimitive(value);
	};
};
/** @internal */
const exitSucceed = /*#__PURE__*/ makeExit({
	op: "Success",
	prop: "value",
	[evaluate](fiber) {
		const cont = fiber.getCont(contA);
		return cont ? cont[contA](this[args], fiber, this) : fiber.yieldWith(this);
	}
});
/** @internal */
const StackTraceKey = { key: "effect/Cause/StackTrace" };
/** @internal */
const InterruptorStackTrace = { key: "effect/Cause/InterruptorStackTrace" };
/** @internal */
const exitFailCause = /*#__PURE__*/ makeExit({
	op: "Failure",
	prop: "cause",
	[evaluate](fiber) {
		let cause = this[args];
		let annotated = false;
		if (fiber.cache.stackFrame) {
			cause = causeAnnotate(cause, { mapUnsafe: /* @__PURE__ */ new Map([[StackTraceKey.key, fiber.cache.stackFrame]]) });
			annotated = true;
		}
		let cont = fiber.getCont(contE);
		const interruptedCause = fiber._interruptedCause;
		if (interruptedCause && fiber.interruptible) {
			let skippedHandler = false;
			while (cont && fiber.interruptible) {
				skippedHandler ||= identifier in cont;
				cont = fiber.getCont(contE);
			}
			if (skippedHandler) cause = causeFromReasons(cause.reasons.filter((reason) => reason._tag !== "Fail"));
			cause = causeAddInterrupts(cause, interruptedCause);
			annotated = true;
		}
		return cont ? cont[contE](cause, fiber, annotated ? void 0 : this) : fiber.yieldWith(annotated ? exitFailCause(cause) : this);
	}
});
/** @internal */
const exitFail = (e) => exitFailCause(causeFail(e));
/** @internal */
const exitDie = (defect) => exitFailCause(causeDie(defect));
/** @internal */
const withFiber$1 = /*#__PURE__*/ makePrimitive({
	op: "WithFiber",
	[evaluate](fiber) {
		return this[args](fiber);
	}
});
/** @internal */
const YieldableError = /*#__PURE__*/ function() {
	class YieldableError extends globalThis.Error {}
	const proto = /*#__PURE__*/ makePrimitiveProto({
		op: "YieldableError",
		[evaluate]() {
			return exitFail(this);
		}
	});
	delete proto.toString;
	Object.assign(YieldableError.prototype, proto);
	return YieldableError;
}();
/** @internal */
const Error$3 = /*#__PURE__*/ function() {
	const plainArgsSymbol = /*#__PURE__*/ Symbol.for("effect/Data/Error/plainArgs");
	return class Base extends YieldableError {
		constructor(args) {
			super(args?.message, args?.cause ? { cause: args.cause } : void 0);
			if (args) {
				assignProperties(this, args);
				Object.defineProperty(this, plainArgsSymbol, {
					value: args,
					enumerable: false
				});
			}
		}
		toJSON() {
			return {
				...this[plainArgsSymbol],
				...this
			};
		}
	};
}();
/** @internal */
const TaggedError$2 = (tag) => {
	class Base extends Error$3 {
		_tag = tag;
	}
	Base.prototype.name = tag;
	return Base;
};
/** @internal */
const DoneTypeId = "~effect/Cause/Done";
/** @internal */
const isDone = (u) => hasProperty(u, DoneTypeId);
const DoneVoid = {
	[DoneTypeId]: DoneTypeId,
	_tag: "Done",
	value: void 0
};
/** @internal */
const Done = (value) => {
	if (value === void 0) return DoneVoid;
	return {
		[DoneTypeId]: DoneTypeId,
		_tag: "Done",
		value
	};
};
const doneVoid = /*#__PURE__*/ exitFail(DoneVoid);
/** @internal */
const done$2 = (value) => {
	if (value === void 0) return doneVoid;
	return exitFail(Done(value));
};
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/option.js
/**
* @since 2.0.0
*/
const TypeId$21 = "~effect/Option";
const CommonProto$1 = {
	[TypeId$21]: { _A: (_) => _ },
	...PipeInspectableProto,
	[Symbol.iterator]() {
		return new SingleShotGen(this);
	}
};
const SomeProto = /*#__PURE__*/ Object.defineProperty(/*#__PURE__*/ Object.assign(/*#__PURE__*/ Object.create(CommonProto$1), {
	_tag: "Some",
	_op: "Some",
	[symbol](that) {
		return isOption(that) && isSome$1(that) && equals$1(this.value, that.value);
	},
	[symbol$1]() {
		return combine(hash(this._tag))(hash(this.value));
	},
	toString() {
		return `some(${format$1(this.value)})`;
	},
	toJSON() {
		return {
			_id: "Option",
			_tag: this._tag,
			value: toJson(this.value)
		};
	}
}), "valueOrUndefined", { get() {
	return this.value;
} });
const NoneHash = /*#__PURE__*/ hash("None");
const NoneProto = /*#__PURE__*/ Object.assign(/*#__PURE__*/ Object.create(CommonProto$1), {
	_tag: "None",
	_op: "None",
	valueOrUndefined: void 0,
	[symbol](that) {
		return isOption(that) && isNone$1(that);
	},
	[symbol$1]() {
		return NoneHash;
	},
	toString() {
		return `none()`;
	},
	toJSON() {
		return {
			_id: "Option",
			_tag: this._tag
		};
	}
});
/** @internal */
const isOption = (input) => hasProperty(input, TypeId$21);
/** @internal */
const isNone$1 = (fa) => fa._tag === "None";
/** @internal */
const isSome$1 = (fa) => fa._tag === "Some";
/** @internal */
const none$1 = /*#__PURE__*/ Object.create(NoneProto);
/** @internal */
const SomeImpl = function(value) {
	this.value = value;
};
SomeImpl.prototype = SomeProto;
/** @internal */
const some$1 = (value) => new SomeImpl(value);
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/result.js
const TypeId$20 = "~effect/Result";
const CommonProto = {
	[TypeId$20]: {
		/* v8 ignore next 2 */
		_A: (_) => _,
		_E: (_) => _
	},
	...PipeInspectableProto,
	[Symbol.iterator]() {
		return new SingleShotGen(this);
	}
};
const SuccessProto = /*#__PURE__*/ Object.assign(/*#__PURE__*/ Object.create(CommonProto), {
	_tag: "Success",
	_op: "Success",
	[symbol](that) {
		return isResult(that) && isSuccess$2(that) && equals$1(this.success, that.success);
	},
	[symbol$1]() {
		return combine(hash(this._tag))(hash(this.success));
	},
	toString() {
		return `success(${format$1(this.success)})`;
	},
	toJSON() {
		return {
			_id: "Result",
			_tag: this._tag,
			value: toJson(this.success)
		};
	}
});
const FailureProto = /*#__PURE__*/ Object.assign(/*#__PURE__*/ Object.create(CommonProto), {
	_tag: "Failure",
	_op: "Failure",
	[symbol](that) {
		return isResult(that) && isFailure$1(that) && equals$1(this.failure, that.failure);
	},
	[symbol$1]() {
		return combine(hash(this._tag))(hash(this.failure));
	},
	toString() {
		return `failure(${format$1(this.failure)})`;
	},
	toJSON() {
		return {
			_id: "Result",
			_tag: this._tag,
			failure: toJson(this.failure)
		};
	}
});
/** @internal */
const isResult = (input) => hasProperty(input, TypeId$20);
/** @internal */
const isFailure$1 = (result) => result._tag === "Failure";
/** @internal */
const isSuccess$2 = (result) => result._tag === "Success";
/** @internal */
const FailureImpl = function(failure) {
	this.failure = failure;
};
FailureImpl.prototype = FailureProto;
/** @internal */
const fail$5 = (failure) => new FailureImpl(failure);
/** @internal */
const SuccessImpl = function(success) {
	this.success = success;
};
SuccessImpl.prototype = SuccessProto;
/** @internal */
const succeed$5 = (success) => new SuccessImpl(success);
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Order.js
/**
* Defines comparison functions for ordered values.
*
* An `Order<A>` compares two `A` values and returns whether the first is less
* than, equal to, or greater than the second. Orders are used for sorting,
* choosing minimum or maximum values, checking ranges, and building ordered data
* structures. This module includes built-in orders, constructors for custom
* orders, tools for reversing and combining comparisons, tuple and struct
* helpers, comparison predicates, clamping, and reducer support.
*
* @stability stable
* @since 2.0.0
*/
/**
* Creates a new `Order` instance from a comparison function.
*
* **When to use**
*
* Use when you need a sorting rule not covered by the built-in orders or input
* mapping helpers, and you can provide a total comparison.
*
* **Details**
*
* Uses reference equality (`===`) as a shortcut: if `self === that`, it returns
* `0` without calling the comparison function. The comparison function should
* return `-1`, `0`, or `1`, and the returned order satisfies total ordering
* laws when the comparison function does.
*
* **Example** (Creating an Order)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* const byAge = Order.make<{ name: string; age: number }>((self, that) => {
*   if (self.age < that.age) return -1
*   if (self.age > that.age) return 1
*   return 0
* })
*
* byAge({ name: "Alice", age: 30 }, { name: "Bob", age: 25 }) // => 1
* byAge({ name: "Alice", age: 25 }, { name: "Bob", age: 30 }) // => -1
* ```
*
* @see {@link mapInput} to transform an order by mapping the input type
* @see {@link combine} to combine multiple orders
* @stability stable
* @category constructors
* @since 2.0.0
*/
function make$13(compare) {
	return (self, that) => self === that ? 0 : compare(self, that);
}
/**
* Order instance for strings that compares them lexicographically using JavaScript's `<` operator.
*
* **When to use**
*
* Use when you need lexicographic string ordering.
*
* **Details**
*
* Uses lexicographic dictionary ordering. The empty string is less than any
* non-empty string, and comparisons are case-sensitive.
*
* **Example** (Ordering strings)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* Order.String("apple", "banana") // => -1
* Order.String("banana", "apple") // => 1
* Order.String("apple", "apple") // => 0
* ```
*
* @see {@link mapInput} to compare objects by a string property
* @see {@link Struct} to combine with other orders for struct comparison
* @stability stable
* @category instances
* @since 4.0.0
*/
const String$4 = /*#__PURE__*/ make$13((self, that) => self < that ? -1 : 1);
/**
* Order instance for numbers that compares them numerically.
*
* **When to use**
*
* Use when you need numeric ordering for numbers.
*
* **Details**
*
* `0` is considered equal to `-0`. All `NaN` values are considered equal to
* each other, and any `NaN` is considered less than any non-`NaN` number. All
* other values use standard numeric comparison.
*
* **Example** (Ordering numbers)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* Order.Number(1, 1) // => 0
* Order.Number(1, 2) // => -1
* Order.Number(2, 1) // => 1
*
* Order.Number(0, -0) // => 0
* Order.Number(NaN, 1) // => -1
* ```
*
* @see {@link mapInput} to compare objects by a number property
* @see {@link BigInt} for bigint comparisons
* @stability stable
* @category instances
* @since 4.0.0
*/
const Number$4 = /*#__PURE__*/ make$13((self, that) => {
	if (globalThis.Number.isNaN(self) && globalThis.Number.isNaN(that)) return 0;
	if (globalThis.Number.isNaN(self)) return -1;
	if (globalThis.Number.isNaN(that)) return 1;
	return self < that ? -1 : 1;
});
/**
* Checks whether one value is strictly less than another according to the given order.
*
* **When to use**
*
* Use when you need a boolean less-than predicate using an `Order`.
*
* **Details**
*
* Returns `true` if the order returns `-1`, meaning the first value is less
* than the second. Equal or greater values return `false`.
*
* **Example** (Checking less-than comparisons)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* const isLessThanNumber = Order.isLessThan(Order.Number)
*
* isLessThanNumber(1, 2) // => true
* isLessThanNumber(2, 1) // => false
* isLessThanNumber(1, 1) // => false
* ```
*
* @see {@link isLessThanOrEqualTo} for non-strict less than or equal
* @see {@link isGreaterThan} for strict greater than
* @stability stable
* @category predicates
* @since 4.0.0
*/
const isLessThan$1 = (O) => dual(2, (self, that) => O(self, that) === -1);
/**
* Checks whether one value is strictly greater than another according to the given order.
*
* **When to use**
*
* Use when you need a boolean greater-than predicate using an `Order`.
*
* **Details**
*
* Returns `true` if the order returns `1`, meaning the first value is greater
* than the second. Equal or lesser values return `false`.
*
* **Example** (Checking greater-than comparisons)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* const isGreaterThanNumber = Order.isGreaterThan(Order.Number)
*
* isGreaterThanNumber(2, 1) // => true
* isGreaterThanNumber(1, 2) // => false
* isGreaterThanNumber(1, 1) // => false
* ```
*
* @see {@link isGreaterThanOrEqualTo} for non-strict greater than or equal
* @see {@link isLessThan} for strict less than
* @stability stable
* @category predicates
* @since 4.0.0
*/
const isGreaterThan$1 = (O) => dual(2, (self, that) => O(self, that) === 1);
/**
* Checks whether one value is less than or equal to another according to the given order.
*
* **When to use**
*
* Use when you need a boolean less-than-or-equal predicate using an `Order`.
*
* **Details**
*
* Returns `true` if the order returns `-1` or `0`, and returns `false` only if
* the order returns `1`.
*
* **Example** (Checking less-than-or-equal comparisons)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* const isLessThanOrEqualToNumber = Order.isLessThanOrEqualTo(Order.Number)
*
* isLessThanOrEqualToNumber(1, 2) // => true
* isLessThanOrEqualToNumber(1, 1) // => true
* isLessThanOrEqualToNumber(2, 1) // => false
* ```
*
* @see {@link isLessThan} for strict less than
* @see {@link isGreaterThan} for strict greater than
* @stability stable
* @category predicates
* @since 4.0.0
*/
const isLessThanOrEqualTo$1 = (O) => dual(2, (self, that) => O(self, that) !== 1);
/**
* Checks whether one value is greater than or equal to another according to the given order.
*
* **When to use**
*
* Use when you need a boolean greater-than-or-equal predicate using an
* `Order`.
*
* **Details**
*
* Returns `true` if the order returns `1` or `0`, and returns `false` only if
* the order returns `-1`.
*
* **Example** (Checking greater-than-or-equal comparisons)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* const isGreaterThanOrEqualToNumber = Order.isGreaterThanOrEqualTo(Order.Number)
*
* isGreaterThanOrEqualToNumber(2, 1) // => true
* isGreaterThanOrEqualToNumber(1, 1) // => true
* isGreaterThanOrEqualToNumber(1, 2) // => false
* ```
*
* @see {@link isGreaterThan} for strict greater than
* @see {@link isLessThanOrEqualTo} for less than or equal
* @stability stable
* @category predicates
* @since 4.0.0
*/
const isGreaterThanOrEqualTo$1 = (O) => dual(2, (self, that) => O(self, that) !== -1);
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Option.js
/**
* Creates an `Option` representing the absence of a value.
*
* **When to use**
*
* Use to represent a missing or uninitialized value, such as returning "no
* result" from a function.
*
* **Details**
*
* - Returns `Option<never>`, which is a subtype of `Option<A>` for any `A`
* - Always returns the same singleton instance
*
* **Example** (Creating an empty Option)
*
* ```ts import.meta.vitest
* import { Option } from "effect"
*
* //      ┌─── Option<never>
* //      ▼
* const noValue = Option.none() // => Option.none()
* ```
*
* @see {@link some} for the opposite operation.
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const none = () => none$1;
/**
* Wraps the given value into an `Option` to represent its presence.
*
* **When to use**
*
* Use to wrap a known present value as `Option`
* - Returning a successful result from a partial function
*
* **Details**
*
* - Always returns `Some<A>`
* - Does not filter `null` or `undefined`; use {@link fromNullishOr} for that
*
* **Example** (Wrapping a value)
*
* ```ts import.meta.vitest
* import { Option } from "effect"
*
* //      ┌─── Option<number>
* //      ▼
* const value = Option.some(1) // => Option.some(1)
* ```
*
* @see {@link none} for the opposite operation.
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const some = some$1;
/**
* Checks whether an `Option` contains a value (`Some`).
*
* **When to use**
*
* Use when you need to branch on a present `Option` before accessing `.value`.
*
* **Details**
*
* - Acts as a type guard, narrowing to `Some<A>`
*
* **Example** (Checking for Some)
*
* ```ts import.meta.vitest
* import { Option } from "effect"
*
* Option.isSome(Option.some(1)) // => true
* Option.isSome(Option.none()) // => false
* ```
*
* @see {@link isNone} for the opposite check.
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isSome = isSome$1;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Result.js
/**
* Creates a `Result` holding a `Success` value.
*
* **Details**
*
* - Use when you have a value and want to lift it into the `Result` type
* - The error type `E` defaults to `never`
*
* **Example** (Wrapping a value)
*
* ```ts import.meta.vitest
* import { Result } from "effect"
*
* Result.succeed(42) // => Result.succeed(42)
* ```
*
* @see {@link fail} to create a Failure
* @see {@link void_ void} for a pre-built `Success<void>`
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const succeed$4 = succeed$5;
/**
* Creates a `Result` holding a `Failure` value.
*
* **When to use**
*
* Use to represent a failed `Result` with a typed failure value.
*
* **Details**
*
* - The success type `A` defaults to `never`
*
* **Example** (Creating a failure)
*
* ```ts import.meta.vitest
* import { Result } from "effect"
*
* Result.fail("Something went wrong") // => Result.fail("Something went wrong")
* ```
*
* @see {@link succeed} to create a Success
* @see {@link mapError} to transform the error
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fail$4 = fail$5;
/**
* Checks whether a `Result` is a `Failure`.
*
* **When to use**
*
* Use to narrow a known `Result` to the `Failure` variant.
*
* **Details**
*
* - Acts as a TypeScript type guard, narrowing to `Failure<A, E>`
* - After narrowing, you can access `.failure` to read the error value
*
* **Example** (Narrowing to failure)
*
* ```ts import.meta.vitest
* import { Result } from "effect"
*
* const result = Result.fail("oops")
*
* if (Result.isFailure(result)) {
*   result.failure // => "oops"
* }
* ```
*
* @see {@link isSuccess} for the opposite check
* @see {@link isResult} to check if a value is any Result
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isFailure = isFailure$1;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Array.js
/**
* Exposes the global array constructor.
*
* **When to use**
*
* Use to access native JavaScript array constructor methods such as `isArray`
* or `from` from the Effect module namespace.
*
* **Example** (Accessing the Array constructor)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.Array === globalThis.Array // => true
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Array$1 = globalThis.Array;
/**
* Converts an `Iterable` to an `Array`.
*
* **When to use**
*
* Use to convert any `Iterable` (Set, Generator, etc.) into an array.
*
* **Details**
*
* If the input is already an array, this returns it by reference without
* copying. Otherwise, it creates a new array from the iterable. Use `copy` if
* you need a fresh array even when the input is already an array.
*
* **Example** (Converting a Set to an array)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.fromIterable(new Set([1, 2, 3])) // => [1, 2, 3]
* ```
*
* @see {@link ensure} — wrap a single value or return an existing array
* @see {@link copy} — create a shallow copy of an array
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fromIterable$1 = (collection) => Array$1.isArray(collection) ? collection : Array$1.from(collection);
/**
* Adds a single element to the end of an iterable, returning a `NonEmptyArray`.
*
* **When to use**
*
* Use when you need to guarantee a non-empty result after adding a required
* trailing value.
*
* **Example** (Appending an element)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.append([1, 2, 3], 4) // => [1, 2, 3, 4]
* ```
*
* @see {@link prepend} — add to the front
* @see {@link appendAll} — append multiple elements
*
* @stability stable
* @category combining
* @since 2.0.0
*/
const append$1 = /*#__PURE__*/ dual(2, (self, last) => [...self, last]);
Array$1.isArray;
/**
* Checks whether a mutable `Array` is non-empty, narrowing the type to
* `NonEmptyArray`.
*
* **When to use**
*
* Use when you need the narrowed value to remain a mutable `Array` after proving
* it has at least one element.
*
* **Example** (Checking for a non-empty array)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.isArrayNonEmpty([]) // => false
* Array.isArrayNonEmpty([1, 2, 3]) // => true
* ```
*
* @see {@link isReadonlyArrayNonEmpty} — readonly variant
* @see {@link isArrayEmpty} — opposite check
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isArrayNonEmpty = isArrayNonEmpty$1;
/**
* Checks whether a `ReadonlyArray` is non-empty, narrowing the type to
* `NonEmptyReadonlyArray`.
*
* **When to use**
*
* Use when you need to prove a readonly array has at least one element without
* requiring mutable array methods afterward.
*
* **Example** (Checking for a non-empty readonly array)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.isReadonlyArrayNonEmpty([]) // => false
* Array.isReadonlyArrayNonEmpty([1, 2, 3]) // => true
* ```
*
* @see {@link isArrayNonEmpty} — mutable variant
* @see {@link isReadonlyArrayEmpty} — opposite check
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isReadonlyArrayNonEmpty = isArrayNonEmpty$1;
/**
* Creates an empty array.
*
* **When to use**
*
* Use to create a typed empty array without allocating placeholder elements.
*
* **Example** (Creating an empty array)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.empty<number>() // => []
* ```
*
* @see {@link of} — create a single-element array
* @see {@link make} — create from multiple values
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const empty$1 = () => [];
/**
* Transforms each element using a function, returning a new array.
*
* **When to use**
*
* Use to transform each element independently while preserving the array shape.
*
* **Details**
*
* The function receives `(element, index)`. The return type preserves
* `NonEmptyArray`.
*
* **Example** (Doubling values)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.map([1, 2, 3], (x) => x * 2) // => [2, 4, 6]
* ```
*
* @see {@link flatMap} — map and flatten
*
* @stability stable
* @category mapping
* @since 2.0.0
*/
const map$3 = /*#__PURE__*/ dual(2, (self, f) => self.map(f));
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/BigDecimal.js
/**
* Decimal numbers and arithmetic for cases where JavaScript `number` rounding
* is not precise enough. A `BigDecimal` stores digits as a `bigint` plus a
* decimal scale, which lets the module parse, compare, add, subtract, multiply,
* divide, round, and format decimal values such as money, quantities, and
* measurements.
*
* Parsed scales are limited to ±10,000, and operations that align scales throw
* a `RangeError` when the scales differ by more than 20,000.
*
* @stability stable
* @since 2.0.0
*/
const TypeId$19 = "~effect/BigDecimal";
const BigDecimalProto = {
	[TypeId$19]: TypeId$19,
	[symbol$1]() {
		const normalized = normalize(this);
		return combine(string$2(String(normalized.value)), number$4(normalized.scale));
	},
	[symbol](that) {
		return isBigDecimal(that) && compare(this, that) === 0;
	},
	toString() {
		return `BigDecimal(${format(this)})`;
	},
	toJSON() {
		return {
			_id: "BigDecimal",
			value: String(this.value),
			scale: this.scale
		};
	},
	[NodeInspectSymbol]() {
		return this.toJSON();
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
/**
* Checks whether a given value is a `BigDecimal`.
*
* **When to use**
*
* Use to validate unknown input and narrow it to `BigDecimal`.
*
* **Example** (Checking BigDecimal values)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* const decimal = BigDecimal.fromNumber(123.45)
* BigDecimal.isBigDecimal(decimal) // => false
* BigDecimal.isBigDecimal(BigDecimal.fromStringUnsafe("123.45")) // => true
* BigDecimal.isBigDecimal(123.45) // => false
* BigDecimal.isBigDecimal("123.45") // => false
* ```
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isBigDecimal = (u) => hasProperty(u, TypeId$19);
/**
* Creates a `BigDecimal` from a `bigint` value and a safe integer scale.
*
* **When to use**
*
* Use to construct a decimal directly from its unscaled integer value and
* decimal scale.
*
* **Gotchas**
*
* Throws a `RangeError` if `scale` is not a safe integer.
*
* **Example** (Creating decimals from bigint and scale)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* // Create 123.45 (12345 with scale 2)
* const decimal = BigDecimal.make(12345n, 2)
* decimal // => BigDecimal.fromStringUnsafe("123.45")
*
* // Create 42 (42 with scale 0)
* const integer = BigDecimal.make(42n, 0)
* integer // => BigDecimal.fromBigInt(42n)
* ```
*
* @see {@link fromBigInt} for constructing an integer decimal from a `bigint`
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const make$12 = (value, scale) => {
	if (!Number.isSafeInteger(scale)) throw new RangeError(`Scale must be a safe integer, got ${scale}`);
	const o = Object.create(BigDecimalProto);
	o.value = value;
	o.scale = scale;
	return o;
};
const makeNormalized = (value, scale) => {
	const o = make$12(value, scale);
	o.normalized = o;
	return o;
};
const bigint0$1 = /*#__PURE__*/ BigInt(0);
const bigint1$1 = /*#__PURE__*/ BigInt(1);
const bigint10$1 = /*#__PURE__*/ BigInt(10);
const zero$1 = /*#__PURE__*/ makeNormalized(bigint0$1, 0);
/**
* Normalizes a given `BigDecimal` by removing trailing zeros.
*
* **When to use**
*
* Use to canonicalize decimals that have equivalent values but different
* internal scales.
*
* **Example** (Normalizing trailing zeros)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* const decimal = BigDecimal.normalize(BigDecimal.fromStringUnsafe("123.00000"))
* const decimalStorage = [decimal.value, decimal.scale] // => [123n, 0]
*
* const largeDecimal = BigDecimal.normalize(BigDecimal.fromStringUnsafe("12300000"))
* const largeDecimalStorage = [largeDecimal.value, largeDecimal.scale] // => [123n, -5]
* ```
*
* @see {@link format} for rendering normalized decimals as strings
*
* @stability stable
* @category scaling
* @since 2.0.0
*/
const normalize = (self) => {
	if (self.normalized === void 0) {
		if (self.value === bigint0$1) self.normalized = zero$1;
		else {
			const digits = `${self.value}`;
			const zeros = trailingZeros(digits);
			self.normalized = makeNormalized(BigInt(digits.slice(0, digits.length - zeros)), self.scale - zeros);
		}
	}
	return self.normalized;
};
const trailingZeros = (digits) => {
	let end = digits.length;
	while (digits[end - 1] === "0") end--;
	return digits.length - end;
};
const MAX_COMPARISON_SCALE_ALIGNMENT = 100;
const comparisonPowersOfTen = [bigint1$1];
const compareBigInt = (self, that) => self === that ? 0 : self < that ? -1 : 1;
const compareMagnitude = (self, that) => {
	const selfDigits = `${self.value < bigint0$1 ? -self.value : self.value}`;
	const thatDigits = `${that.value < bigint0$1 ? -that.value : that.value}`;
	const exponentDifference = BigInt(selfDigits.length - thatDigits.length) - BigInt(self.scale) + BigInt(that.scale);
	if (exponentDifference !== bigint0$1) return exponentDifference < bigint0$1 ? -1 : 1;
	const length = Math.max(selfDigits.length, thatDigits.length);
	return String$4(selfDigits.padEnd(length, "0"), thatDigits.padEnd(length, "0"));
};
const compare = (self, that) => {
	if (self.scale === that.scale) return compareBigInt(self.value, that.value);
	const selfSign = sign(self);
	const thatSign = sign(that);
	if (selfSign !== thatSign) return selfSign < thatSign ? -1 : 1;
	if (selfSign === 0) return 0;
	const scaleDifference = self.scale - that.scale;
	const absoluteScaleDifference = Math.abs(scaleDifference);
	if (absoluteScaleDifference > MAX_COMPARISON_SCALE_ALIGNMENT) return selfSign === -1 ? compareMagnitude(that, self) : compareMagnitude(self, that);
	const powerOfTen = comparisonPowersOfTen[absoluteScaleDifference] ??= bigint10$1 ** BigInt(absoluteScaleDifference);
	return scaleDifference > 0 ? compareBigInt(self.value, that.value * powerOfTen) : compareBigInt(self.value * powerOfTen, that.value);
};
/**
* Determines the sign of a given `BigDecimal`.
*
* **When to use**
*
* Use to classify a `BigDecimal` as negative, zero, or positive.
*
* **Example** (Reading decimal signs)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* BigDecimal.sign(BigDecimal.fromStringUnsafe("-5")) // => -1
* BigDecimal.sign(BigDecimal.fromStringUnsafe("0")) // => 0
* BigDecimal.sign(BigDecimal.fromStringUnsafe("5")) // => 1
* ```
*
* @stability stable
* @category math
* @since 2.0.0
*/
const sign = (n) => n.value === bigint0$1 ? 0 : n.value < bigint0$1 ? -1 : 1;
/**
* Formats a `BigDecimal` as a string.
*
* **When to use**
*
* Use to render a `BigDecimal` as plain decimal text when possible.
*
* **Details**
*
* The value is normalized before formatting. Scientific notation is used when
* the absolute value of the normalized scale is at least `16`; otherwise plain
* decimal notation is used.
*
* **Example** (Formatting decimals)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* BigDecimal.format(BigDecimal.fromStringUnsafe("-5")) // => "-5"
* BigDecimal.format(BigDecimal.fromStringUnsafe("123.456")) // => "123.456"
* BigDecimal.format(BigDecimal.fromStringUnsafe("-0.00000123")) // => "-0.00000123"
* ```
*
* @see {@link toExponential} for always rendering scientific notation
*
* @stability stable
* @category converting
* @since 2.0.0
*/
const format = (n) => {
	const normalized = normalize(n);
	if (Math.abs(normalized.scale) >= 16) return toExponential(normalized);
	const negative = normalized.value < bigint0$1;
	const absolute = `${negative ? -normalized.value : normalized.value}`;
	const digits = normalized.scale > 0 ? absolute.padStart(normalized.scale + 1, "0") : absolute.padEnd(absolute.length - normalized.scale, "0");
	const point = digits.length - normalized.scale;
	const complete = normalized.scale > 0 ? `${digits.slice(0, point)}.${digits.slice(point)}` : digits;
	return negative ? `-${complete}` : complete;
};
/**
* Formats a given `BigDecimal` as a `string` in scientific notation.
*
* **When to use**
*
* Use to render a `BigDecimal` in scientific notation.
*
* **Example** (Formatting decimals exponentially)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* BigDecimal.toExponential(BigDecimal.make(123456n, -5)) // => "1.23456e+10"
* ```
*
* @see {@link format} for plain decimal formatting when possible
*
* @stability stable
* @category converting
* @since 3.11.0
*/
const toExponential = (n) => {
	if (isZero(n)) return "0e+0";
	const normalized = normalize(n);
	const digits = `${normalized.value}`;
	const point = normalized.value < bigint0$1 ? 2 : 1;
	const head = digits.slice(0, point);
	const tail = digits.slice(point);
	const exp = tail.length - normalized.scale;
	return `${head}${tail === "" ? "" : `.${tail}`}e${exp >= 0 ? "+" : ""}${exp}`;
};
/**
* Checks whether a given `BigDecimal` is `0`.
*
* **When to use**
*
* Use to test whether a `BigDecimal` is exactly zero.
*
* **Example** (Checking zero decimals)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* BigDecimal.isZero(BigDecimal.fromStringUnsafe("0")) // => true
* BigDecimal.isZero(BigDecimal.fromStringUnsafe("1")) // => false
* ```
*
* @stability stable
* @category predicates
* @since 2.0.0
*/
const isZero = (n) => n.value === bigint0$1;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Effectable.js
/**
* Create a low-level `Effect` prototype.
*
* **When to use**
*
* Use when you need to create a custom Effect-like value without extending a
* class, by providing a label and an evaluate function that receives the
* current fiber.
*
* **Details**
*
* When the effect is evaluated, it calls `evaluate` with the current fiber.
*
* @see {@link Class} for a class-based approach to defining custom Effect values
* @see {@link Mixin} for wrapping an existing class constructor
*
* @stability stable
* @category prototypes
* @since 4.0.0
*/
const Prototype = (options) => makePrimitiveProto({
	op: options.label,
	[evaluate]: options.evaluate
});
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Context.js
/**
* Runtime type identifier attached to `Context` service keys and used by
* `isKey` to recognize them.
*
* @stability stable
* @category type IDs
* @since 4.0.0
*/
const ServiceTypeId = "~effect/Context/Service";
/**
* Creates a `Context` service key.
*
* **When to use**
*
* Use when you need to define a context service key for a dependency that must
* be provided by the surrounding context.
*
* **Details**
*
* Call `Context.Service("Key")` for a function-style key, or use the two-stage
* form `Context.Service<Self, Shape>()("Key")` for class-style service
* declarations. The returned key can be yielded as an Effect and passed to
* `Context.make`, `Context.add`, and the Context getter functions.
*
* **Gotchas**
*
* The string key is the runtime identity of the service. Reusing the same key
* string for unrelated services makes them occupy the same slot in a
* `Context`.
*
* **Example** (Creating service keys)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* // Create a simple service
* const Database = Context.Service<{
*   query: (sql: string) => string
* }>("Database")
*
* // Create a service class
* class Config extends Context.Service<Config, {
*   port: number
* }>()("Config") {}
*
* // Use the services to create contexts
* const db = Context.make(Database, {
*   query: (sql) => `Result: ${sql}`
* })
* const config = Context.make(Config, { port: 8080 })
* Context.get(db, Database).query("SELECT 1") // => "Result: SELECT 1"
* Context.get(config, Config).port // => 8080
* ```
*
* @see {@link Reference} for service keys with default values
*
* @stability stable
* @category services
* @since 4.0.0
*/
const Service = function() {
	function KeyClass() {}
	const self = KeyClass;
	Object.setPrototypeOf(self, ServiceProto);
	const init = (key, options) => {
		self.key = key;
		if (options?.defaultValue) {
			self[ReferenceTypeId] = ReferenceTypeId;
			self.defaultValue = options.defaultValue;
		}
		if (options?.make) self.make = options.make;
		if (options?.fiberCached) cacheKeys.add(key);
		return self;
	};
	return arguments.length > 0 ? init(arguments[0], arguments[1]) : init;
};
const ServiceProto = {
	[ServiceTypeId]: ServiceTypeId,
	.../*#__PURE__*/ Prototype({
		label: "Service",
		evaluate(fiber) {
			return exitSucceed(get(fiber.context, this));
		}
	}),
	toJSON() {
		return {
			_id: "Service",
			key: this.key
		};
	},
	of(self) {
		return self;
	},
	context(self) {
		return make$11(this, self);
	},
	use(f) {
		return withFiber$1((fiber) => f(get(fiber.context, this)));
	},
	useSync(f) {
		return withFiber$1((fiber) => exitSucceed(f(get(fiber.context, this))));
	}
};
const cacheKeys = /*#__PURE__*/ new Set();
const ReferenceTypeId = "~effect/Context/Reference";
const TypeId$18 = "~effect/Context";
const FlattenAfterBaseHits = 8;
const makeImpl = (cacheRoot, base, overlay, depth, maxDepth) => {
	const self = Object.create(Proto$5);
	self.cacheRoot = cacheRoot ?? self;
	self.base = base;
	self.overlay = overlay;
	self.depth = depth;
	self._flat = void 0;
	self.baseHits = 0;
	self.maxDepth = maxDepth;
	return self;
};
const applyOverlays = (map, overlay) => {
	if (!overlay) return;
	applyOverlays(map, overlay.parent);
	map.set(overlay.key, overlay.value);
};
const flatten = (self) => {
	if (self._flat) return self._flat;
	if (!self.overlay) return self._flat = self.base;
	const map = new Map(self.base);
	applyOverlays(map, self.overlay);
	return self._flat = map;
};
const withFlat = (self, f) => {
	const map = new Map(self.mapUnsafe);
	f(map);
	return makeImpl(void 0, map, void 0, 0, self.maxDepth);
};
const notFound = /*#__PURE__*/ Symbol();
const lookup = (self, key) => {
	const impl = self;
	for (let overlay = impl.overlay; overlay; overlay = overlay.parent) if (overlay.key === key) return overlay.value;
	const value = impl.base.get(key);
	if (value === void 0 && !impl.base.has(key)) return notFound;
	if (impl.overlay && ++impl.baseHits >= impl.base.size && impl.baseHits >= FlattenAfterBaseHits) {
		impl.base = flatten(impl);
		impl.overlay = void 0;
		impl.depth = 0;
	}
	return value;
};
/**
* Creates a `Context` from an existing service map.
*
* **When to use**
*
* Use when constructing a low-level `Context` from a trusted map whose lifecycle
* you control.
*
* **Details**
*
* `options.maxDepth` sets how many `add` calls are kept as overlays before the
* context is rebased into a flat map (default `8`). Lower values make lookups
* cheaper, higher values make additions to a large base cheaper. Invalid
* values fall back to the default.
*
* Contexts derived with `add`, `pick`, `omit`, and `merge` keep the setting
* (`merge` from its first non-empty argument). `make`, `empty`, and `mergeAll`
* use the default. Layers are combined with `mergeAll`, so apply it to the
* final context rather than inside a layer.
*
* **Gotchas**
*
* Excessively large `maxDepth` values can cause stack overflow when overlays
* are materialized or rebased. Choose a modest value.
*
* The provided map is retained without copying and must not be mutated after
* construction. Prefer `empty`, `make`, `add`, or `merge` for normal Context
* construction.
*
* **Example** (Creating a context from a map)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* // Create a context from a Map (unsafe)
* const map = new Map([
*   ["Logger", { log: (_msg: string) => {} }]
* ])
*
* const context = Context.makeUnsafe(map)
* context.mapUnsafe.size // => 1
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const makeUnsafe$5 = (mapUnsafe, options) => {
	const maxDepth = options?.maxDepth;
	return makeImpl(void 0, mapUnsafe, void 0, 0, Number.isInteger(maxDepth) && maxDepth >= 0 ? maxDepth : 8);
};
const Proto$5 = {
	get mapUnsafe() {
		return flatten(this);
	},
	...PipeInspectableProto,
	[TypeId$18]: { _Services: (_) => _ },
	toJSON() {
		return {
			_id: "Context",
			services: Array.from(this.mapUnsafe).map(([key, value]) => ({
				key,
				value
			}))
		};
	},
	[symbol](that) {
		if (!isContext(that)) return false;
		const self = this.mapUnsafe;
		const other = that.mapUnsafe;
		if (self.size !== other.size) return false;
		for (const [key, value] of self) if (!other.has(key) || !equals$1(value, other.get(key))) return false;
		return true;
	},
	[symbol$1]() {
		return number$4(this.mapUnsafe.size);
	}
};
/** @internal */
const hasSameCache = (self, that) => self.cacheRoot === that.cacheRoot;
/**
* Checks whether the provided argument is a `Context`.
*
* **When to use**
*
* Use to narrow an unknown value before passing it to APIs that require a
* `Context`.
*
* **Details**
*
* This checks the runtime `Context` marker and does not inspect which services
* the context contains.
*
* **Gotchas**
*
* This guard only proves that the value is a `Context`; it does not prove that
* any specific service is present.
*
* **Example** (Checking for contexts)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
* Context.isContext(Context.empty()) // => true
* ```
*
* @see {@link isKey} for checking service keys
* @see {@link isReference} for checking references with defaults
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isContext = (u) => hasProperty(u, TypeId$18);
/**
* Checks whether the provided argument is a `Reference`.
*
* **Example** (Checking for references)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* const LoggerRef = Context.Reference("Logger", {
*   defaultValue: () => ({ log: (_msg: string) => {} })
* })
*
* Context.isReference(LoggerRef) // => true
* Context.isReference(Context.Service("Key")) // => false
* ```
*
* @stability stable
* @category guards
* @since 3.11.0
*/
const isReference = (u) => !!u[ReferenceTypeId];
/**
* Returns an empty `Context`.
*
* **Example** (Creating an empty context)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
* Context.empty().mapUnsafe.size // => 0
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const empty = () => emptyContext;
const emptyContext = /*#__PURE__*/ makeUnsafe$5(/*#__PURE__*/ new Map());
/**
* Creates a new `Context` with a single service associated to the key.
*
* **Example** (Creating a context with one service)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* const Port = Context.Service<{ PORT: number }>("Port")
*
* const context = Context.make(Port, { PORT: 8080 })
*
* Context.get(context, Port).PORT // => 8080
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const make$11 = (key, service) => makeUnsafe$5(/* @__PURE__ */ new Map([[key.key, service]]));
/**
* Adds a service to a given `Context`.
*
* **When to use**
*
* Use when you need to store a known service value in a `Context`.
*
* **Details**
*
* If the context already contains the same service key, the new service
* replaces the previous one.
*
* **Example** (Adding a service to a context)
*
* ```ts import.meta.vitest
* import { Context, pipe } from "effect"
*
* const Port = Context.Service<{ PORT: number }>("Port")
* const Timeout = Context.Service<{ TIMEOUT: number }>("Timeout")
*
* const someContext = Context.make(Port, { PORT: 8080 })
*
* const context = pipe(
*   someContext,
*   Context.add(Timeout, { TIMEOUT: 5000 })
* )
*
* const values = [Context.get(context, Port).PORT, Context.get(context, Timeout).TIMEOUT]
* values // => [8080, 5000]
* ```
*
* @see {@link addOrOmit} for adding or removing a service from an `Option`
*
* @stability stable
* @category combining
* @since 2.0.0
*/
const add = /*#__PURE__*/ dual(3, (self, key, service) => addUnsafe(self, key.key, service));
/**
* Adds a service by key to a given `Context` using a string key.
*
* @stability stable
* @category combining
* @since 4.0.0
*/
const addUnsafe = (self, key, service) => {
	const impl = self;
	const cacheRoot = cacheKeys.has(key) ? void 0 : impl.cacheRoot;
	if (impl.depth >= impl.maxDepth) {
		const map = new Map(impl._flat ?? impl.base);
		if (!impl._flat) applyOverlays(map, impl.overlay);
		map.set(key, service);
		return makeImpl(cacheRoot, map, void 0, 0, impl.maxDepth);
	}
	return makeImpl(cacheRoot, impl.base, {
		key,
		value: service,
		parent: impl.overlay
	}, impl.depth + 1, impl.maxDepth);
};
/** @internal */
const getOrUndefinedUnsafe = (self, key) => {
	const value = lookup(self, key);
	return value === notFound ? void 0 : value;
};
/**
* Gets a service from the context that corresponds to the given key.
*
* **When to use**
*
* Use when you need type-checked access to a service already included in the
* context type.
*
* **Example** (Getting a service from a context)
*
* ```ts import.meta.vitest
* import { Context, pipe } from "effect"
*
* const Port = Context.Service<{ PORT: number }>("Port")
* const Timeout = Context.Service<{ TIMEOUT: number }>("Timeout")
*
* const context = pipe(
*   Context.make(Port, { PORT: 8080 }),
*   Context.add(Timeout, { TIMEOUT: 5000 })
* )
*
* Context.get(context, Timeout).TIMEOUT // => 5000
* ```
*
* @see {@link getOption} for optional service access
* @see {@link getOrElse} for fallback values
*
* @stability stable
* @category getters
* @since 2.0.0
*/
const get = /* @__PURE__ */ dual(2, (self, service) => {
	const value = lookup(self, service.key);
	if (value === notFound) {
		if (isReference(service)) return getDefaultValue(service);
		throw serviceNotFoundError(service);
	}
	return value;
});
const defaultValueCacheKey = "~effect/Context/defaultValue";
const getDefaultValue = (ref) => {
	if (defaultValueCacheKey in ref) return ref[defaultValueCacheKey];
	return ref[defaultValueCacheKey] = ref.defaultValue();
};
const serviceNotFoundError = (service) => {
	const error = /* @__PURE__ */ new Error(`Service not found${service.key ? `: ${String(service.key)}` : ""}`);
	if (error.stack) {
		const lines = error.stack.split("\n");
		lines.splice(1, 3);
		error.stack = lines.join("\n");
	}
	return error;
};
/**
* Merges two `Context`s into one.
*
* **When to use**
*
* Use when you need to combine two contexts.
*
* **Details**
*
* When both contexts contain the same service key, the service from `that`
* overrides the service from `self`.
*
* **Example** (Merging two contexts)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* const Port = Context.Service<{ PORT: number }>("Port")
* const Timeout = Context.Service<{ TIMEOUT: number }>("Timeout")
*
* const firstContext = Context.make(Port, { PORT: 8080 })
* const secondContext = Context.make(Timeout, { TIMEOUT: 5000 })
*
* const context = Context.merge(firstContext, secondContext)
*
* const values = [Context.get(context, Port).PORT, Context.get(context, Timeout).TIMEOUT]
* values // => [8080, 5000]
* ```
*
* @see {@link mergeAll} for merging more than two contexts at once
*
* @stability stable
* @category combining
* @since 2.0.0
*/
const merge$1 = /*#__PURE__*/ dual(2, (self, that) => {
	if (self.mapUnsafe.size === 0) return that;
	if (that.mapUnsafe.size === 0) return self;
	return withFlat(self, (map) => that.mapUnsafe.forEach((value, key) => map.set(key, value)));
});
/**
* Creates a context key with a default value.
*
* **When to use**
*
* Use when you need to define a context key with a lazily computed default
* value.
*
* **Details**
*
* `Context.Reference` allows you to create a key that can hold a value. You
* can provide a default value for the service, which will automatically be used
* when the context is accessed, or override it with a custom implementation
* when needed. The default value is computed lazily and cached on the
* reference.
*
* **Example** (Creating references with default values)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* // Create a reference with a default value
* const messages: Array<string> = []
* const LoggerRef = Context.Reference("Logger", {
*   defaultValue: () => ({ log: (msg: string) => messages.push(`Default: ${msg}`) })
* })
*
* // The reference provides the default value when accessed from an empty context
* const context = Context.empty()
* const logger = Context.get(context, LoggerRef)
*
* // You can also override the default value
* const customContext = Context.make(LoggerRef, {
*   log: (msg: string) => messages.push(`Custom: ${msg}`)
* })
* const customLogger = Context.get(customContext, LoggerRef)
* logger.log("default")
* customLogger.log("message")
* messages // => ["Default: default", "Custom: message"]
* ```
*
* @see {@link Service} for required services without default values
*
* @stability stable
* @category services
* @since 3.11.0
*/
const Reference = Service;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Duration.js
const TypeId$17 = "~effect/Duration";
const bigint0 = /*#__PURE__*/ BigInt(0);
const bigint1 = /*#__PURE__*/ BigInt(1);
const bigint2 = /*#__PURE__*/ BigInt(2);
const bigint10 = /*#__PURE__*/ BigInt(10);
const bigint1e3 = /*#__PURE__*/ BigInt(1e3);
const bigint1e6 = /*#__PURE__*/ BigInt(1e6);
const bigint1e9 = /*#__PURE__*/ BigInt(1e9);
const bigintNanosPerMinute = /*#__PURE__*/ BigInt(6e10);
const bigintNanosPerHour = /*#__PURE__*/ BigInt(36e11);
const bigintNanosPerDay = /*#__PURE__*/ BigInt(864e11);
const bigintNanosPerWeek = /*#__PURE__*/ BigInt(6048e11);
const roundTiesAwayFromZero = (input) => BigInt(Math.sign(input) * Math.round(Math.abs(input)));
const roundMillisToNanos = (millis) => Number.isInteger(millis) ? BigInt(millis) * bigint1e6 : roundTiesAwayFromZero(millis * 1e6);
const fromNanosNumber = (nanos) => make$10(Number.isFinite(nanos) ? roundTiesAwayFromZero(nanos) : nanos);
const parseNanos = (input, scale) => {
	const decimalIndex = input.indexOf(".");
	if (decimalIndex === -1) return BigInt(input) * scale;
	const isNegative = input[0] === "-";
	const fractional = input.slice(decimalIndex + 1);
	const fractionalScale = bigint10 ** BigInt(fractional.length);
	const scaled = (BigInt(input.slice(isNegative ? 1 : 0, decimalIndex)) * fractionalScale + BigInt(fractional)) * scale;
	return divideTiesAwayFromZero(isNegative ? -scaled : scaled, fractionalScale);
};
const divideTiesAwayFromZero = (numerator, denominator) => {
	const absolute = numerator < bigint0 ? -numerator : numerator;
	const rounded = absolute / denominator + (absolute % denominator * bigint2 >= denominator ? bigint1 : bigint0);
	return numerator < bigint0 ? -rounded : rounded;
};
const fromMillisString = (input, scale) => {
	const millis = Number(input) * scale;
	if (!Number.isFinite(millis)) return make$10(millis);
	const nanos = parseNanos(input, BigInt(scale) * bigint1e6);
	return make$10(Number.isInteger(millis) && BigInt(millis) * bigint1e6 === nanos ? millis : nanos);
};
const DURATION_REGEXP = /^(-?\d+(?:\.\d+)?)\s+(nanos?|micros?|millis?|seconds?|minutes?|hours?|days?|weeks?)$/;
/**
* Decodes a `Duration.Input` into a `Duration`.
*
* **When to use**
*
* Use when the input has already been validated or comes from a trusted source
* and throwing is acceptable for invalid duration syntax.
*
* **Gotchas**
*
* If the input is not a valid `Duration.Input`, it throws an error.
*
* **Example** (Decoding duration inputs)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.fromInputUnsafe(1000) // => Duration.millis(1000)
* Duration.fromInputUnsafe("5 seconds") // => Duration.seconds(5)
* Duration.fromInputUnsafe("Infinity") // => Duration.infinity
* Duration.fromInputUnsafe([2, 500_000_000]) // => Duration.nanos(2_500_000_000n)
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromInputUnsafe = (input) => {
	switch (typeof input) {
		case "number": return millis(input);
		case "bigint": return nanos(input);
		case "string": {
			if (input === "Infinity") return infinity;
			if (input === "-Infinity") return negativeInfinity;
			const match = DURATION_REGEXP.exec(input);
			if (!match) break;
			const [_, valueStr, unit] = match;
			if (unit === "nano" || unit === "nanos") return nanos(parseNanos(valueStr, bigint1));
			if (unit === "micro" || unit === "micros") return nanos(parseNanos(valueStr, bigint1e3));
			switch (unit) {
				case "milli":
				case "millis": return fromMillisString(valueStr, 1);
				case "second":
				case "seconds": return fromMillisString(valueStr, 1e3);
				case "minute":
				case "minutes": return fromMillisString(valueStr, 6e4);
				case "hour":
				case "hours": return fromMillisString(valueStr, 36e5);
				case "day":
				case "days": return fromMillisString(valueStr, 864e5);
				case "week":
				case "weeks": return fromMillisString(valueStr, 6048e5);
			}
			break;
		}
		case "object": {
			if (input === null) break;
			if (TypeId$17 in input) return input;
			if (Array.isArray(input)) {
				if (input.length !== 2 || !input.every(isNumber)) return invalid$1(input);
				if (Number.isNaN(input[0]) || Number.isNaN(input[1])) return zero;
				if (input[0] === -Infinity || input[1] === -Infinity) return negativeInfinity;
				if (input[0] === Infinity || input[1] === Infinity) return infinity;
				const nanos = input[0] * 1e9 + input[1];
				return Number.isFinite(nanos) && Number.isInteger(input[0]) && Number.isInteger(input[1]) ? make$10(BigInt(input[0]) * bigint1e9 + BigInt(input[1])) : fromNanosNumber(nanos);
			}
			const obj = input;
			let millis = 0;
			if (obj.weeks) millis += obj.weeks * 6048e5;
			if (obj.days) millis += obj.days * 864e5;
			if (obj.hours) millis += obj.hours * 36e5;
			if (obj.minutes) millis += obj.minutes * 6e4;
			if (obj.seconds) millis += obj.seconds * 1e3;
			if (obj.milliseconds) millis += obj.milliseconds;
			if (!obj.microseconds && !obj.nanoseconds) {
				if (!Number.isFinite(millis)) return make$10(millis);
				const exact = sumObjectNanos(obj);
				return exact === void 0 || BigInt(millis) * bigint1e6 === exact ? make$10(millis) : make$10(exact);
			}
			const nanos = millis * 1e6 + (obj.microseconds ?? 0) * 1e3 + (obj.nanoseconds ?? 0);
			const exact = Number.isFinite(nanos) ? sumObjectNanos(obj) : void 0;
			return exact === void 0 ? fromNanosNumber(nanos) : make$10(exact);
		}
	}
	return invalid$1(input);
};
const isIntegerOrUnset = (n) => !n || Number.isInteger(n);
const sumObjectNanos = (obj) => {
	if (!isIntegerOrUnset(obj.weeks) || !isIntegerOrUnset(obj.days) || !isIntegerOrUnset(obj.hours) || !isIntegerOrUnset(obj.minutes) || !isIntegerOrUnset(obj.seconds) || !isIntegerOrUnset(obj.milliseconds) || !isIntegerOrUnset(obj.microseconds) || !isIntegerOrUnset(obj.nanoseconds)) return void 0;
	let nanos = bigint0;
	if (obj.weeks) nanos += BigInt(obj.weeks) * bigintNanosPerWeek;
	if (obj.days) nanos += BigInt(obj.days) * bigintNanosPerDay;
	if (obj.hours) nanos += BigInt(obj.hours) * bigintNanosPerHour;
	if (obj.minutes) nanos += BigInt(obj.minutes) * bigintNanosPerMinute;
	if (obj.seconds) nanos += BigInt(obj.seconds) * bigint1e9;
	if (obj.milliseconds) nanos += BigInt(obj.milliseconds) * bigint1e6;
	if (obj.microseconds) nanos += BigInt(obj.microseconds) * bigint1e3;
	if (obj.nanoseconds) nanos += BigInt(obj.nanoseconds);
	return nanos;
};
const invalid$1 = (input) => {
	throw new Error(`Invalid Input: ${input}`);
};
const zeroDurationValue = {
	_tag: "Millis",
	millis: 0
};
const infinityDurationValue = { _tag: "Infinity" };
const negativeInfinityDurationValue = { _tag: "NegativeInfinity" };
const DurationProto = {
	[TypeId$17]: TypeId$17,
	[symbol$1]() {
		switch (this.value._tag) {
			case "Millis": return hash(roundMillisToNanos(this.value.millis));
			case "Nanos": return hash(this.value.nanos);
			default: return structure(this.value);
		}
	},
	[symbol](that) {
		return isDuration(that) && equals(this, that);
	},
	toString() {
		switch (this.value._tag) {
			case "Infinity": return "Infinity";
			case "NegativeInfinity": return "-Infinity";
			case "Nanos": return `${this.value.nanos} nanos`;
			case "Millis": return `${this.value.millis} millis`;
		}
	},
	toJSON() {
		switch (this.value._tag) {
			case "Millis": return {
				_id: "Duration",
				_tag: "Millis",
				millis: this.value.millis
			};
			case "Nanos": return {
				_id: "Duration",
				_tag: "Nanos",
				nanos: String(this.value.nanos)
			};
			case "Infinity": return {
				_id: "Duration",
				_tag: "Infinity"
			};
			case "NegativeInfinity": return {
				_id: "Duration",
				_tag: "NegativeInfinity"
			};
		}
	},
	[NodeInspectSymbol]() {
		return this.toJSON();
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
const make$10 = (input) => {
	const duration = Object.create(DurationProto);
	if (typeof input === "number") {
		if (isNaN(input) || input === 0 || Object.is(input, -0)) duration.value = zeroDurationValue;
		else if (!Number.isFinite(input)) duration.value = input > 0 ? infinityDurationValue : negativeInfinityDurationValue;
		else if (!Number.isInteger(input)) duration.value = {
			_tag: "Nanos",
			nanos: roundMillisToNanos(input)
		};
		else duration.value = {
			_tag: "Millis",
			millis: input
		};
	} else if (input === bigint0) duration.value = zeroDurationValue;
	else duration.value = {
		_tag: "Nanos",
		nanos: input
	};
	return duration;
};
/**
* Checks whether a value is a Duration.
*
* **Example** (Checking for durations)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.isDuration(Duration.seconds(1)) // => true
* Duration.isDuration(1000) // => false
* ```
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isDuration = (u) => hasProperty(u, TypeId$17);
/**
* A Duration representing zero time.
*
* **Example** (Referencing the zero duration)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toMillis(Duration.zero) // => 0
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const zero = /*#__PURE__*/ make$10(0);
/**
* A Duration representing infinite time.
*
* **Example** (Referencing infinite duration)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toMillis(Duration.infinity) // => Infinity
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const infinity = /*#__PURE__*/ make$10(Infinity);
/**
* A Duration representing negative infinite time.
*
* **Example** (Referencing negative infinite duration)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toMillis(Duration.negativeInfinity) // => -Infinity
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const negativeInfinity = /*#__PURE__*/ make$10(-Infinity);
/**
* Creates a Duration from nanoseconds.
*
* **Example** (Creating durations from nanoseconds)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.nanos(500_000_000n) // => Duration.nanos(500_000_000n)
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const nanos = (nanos) => make$10(nanos);
/**
* Creates a Duration from milliseconds.
*
* **Example** (Creating durations from milliseconds)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toMillis(Duration.millis(1000)) // => 1000
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const millis = (millis) => make$10(millis);
/**
* Gets the duration in nanoseconds as a bigint.
*
* **When to use**
*
* Use when the duration is known to be finite and you need the nanosecond value
* as a `bigint`.
*
* **Details**
*
* Millisecond-backed fractional durations are rounded to the nearest
* nanosecond, with ties away from zero.
*
* **Gotchas**
*
* If the duration is infinite, it throws an error.
*
* **Example** (Reading nanoseconds unsafely)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toNanosUnsafe(Duration.seconds(2)) // => 2_000_000_000n
*
* // Duration.toNanosUnsafe(Duration.infinity)
* // throws Error: "Cannot convert infinite duration to nanos"
* ```
*
* @stability stable
* @category getters
* @since 4.0.0
*/
const toNanosUnsafe = (input) => {
	const self = fromInputUnsafe(input);
	switch (self.value._tag) {
		case "Infinity":
		case "NegativeInfinity": throw new Error("Cannot convert infinite duration to nanos");
		case "Nanos": return self.value.nanos;
		case "Millis": return roundMillisToNanos(self.value.millis);
	}
};
/**
* Pattern matches on two `Duration`s, providing handlers that receive both values.
*
* **Example** (Pattern matching on duration pairs)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.matchPair(Duration.seconds(3), Duration.seconds(2), {
*   onMillis: (a, b) => a + b,
*   onNanos: (a, b) => Number(a + b),
*   onInfinity: () => Infinity
* }) // => 5000
* ```
*
* @stability stable
* @category pattern matching
* @since 4.0.0
*/
const matchPair = /*#__PURE__*/ dual(3, (self, that, options) => {
	if (self.value._tag === "Infinity" || self.value._tag === "NegativeInfinity" || that.value._tag === "Infinity" || that.value._tag === "NegativeInfinity") return options.onInfinity(self, that);
	if (self.value._tag === "Millis") return that.value._tag === "Millis" ? options.onMillis(self.value.millis, that.value.millis) : options.onNanos(toNanosUnsafe(self), that.value.nanos);
	else return options.onNanos(self.value.nanos, toNanosUnsafe(that));
});
/**
* Provides an `Equivalence` instance for comparing `Duration` values.
*
* **Example** (Comparing durations for equivalence)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.Equivalence(Duration.seconds(5), Duration.millis(5000)) // => true
* ```
*
* @stability stable
* @category instances
* @since 2.0.0
*/
const Equivalence$3 = (self, that) => matchPair(self, that, {
	onMillis: (self, that) => self === that,
	onNanos: (self, that) => self === that,
	onInfinity: (self, that) => self.value._tag === that.value._tag
});
/**
* Checks whether two Durations are equal.
*
* **Example** (Checking duration equality)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.equals(Duration.seconds(5), Duration.millis(5000)) // => true
* ```
*
* @stability stable
* @category predicates
* @since 2.0.0
*/
const equals = /*#__PURE__*/ dual(2, (self, that) => Equivalence$3(self, that));
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Scheduler.js
/**
* Controls how runnable Effect fiber tasks are dispatched.
*
* A scheduler decides how tasks are queued, when queued tasks run, and when a
* fiber should pause so other work can continue. This module includes the
* scheduler service reference, the default `MixedScheduler`, dispatcher types
* for queued tasks, and references for tuning or disabling automatic scheduler
* yields.
*
* @stability stable
* @since 2.0.0
*/
/**
* Context reference for the scheduler used by the Effect runtime.
*
* **When to use**
*
* Use when you need to replace scheduling behavior globally in tests or runtime
* setup, such as forcing deterministic task dispatch.
*
* **Details**
*
* The default value creates a `MixedScheduler`. Provide this service to
* customize execution mode, task dispatching, or yield behavior.
*
* @stability stable
* @category services
* @since 2.0.0
*/
const Scheduler = /*#__PURE__*/ Reference("effect/Scheduler", {
	fiberCached: true,
	defaultValue: () => new MixedScheduler()
});
const setMicrotask = (f) => {
	let cancelled = false;
	Promise.resolve().then(() => {
		if (!cancelled) f();
	});
	return () => {
		cancelled = true;
	};
};
const setTimer = "setImmediate" in globalThis ? (f) => {
	const timer = globalThis.setImmediate(f);
	return () => globalThis.clearImmediate(timer);
} : (f) => {
	const timer = setTimeout(f, 0);
	return () => clearTimeout(timer);
};
const setImmediate = (f) => {
	try {
		return setTimer(f);
	} catch {
		return setMicrotask(f);
	}
};
var PriorityBuckets = class {
	buckets = [];
	scheduleTask(task, priority) {
		const buckets = this.buckets;
		const len = buckets.length;
		let bucket;
		let index = 0;
		for (; index < len; index++) {
			if (buckets[index][0] > priority) break;
			bucket = buckets[index];
		}
		if (bucket && bucket[0] === priority) bucket[1].push(task);
		else if (index === len) buckets.push([priority, [task]]);
		else buckets.splice(index, 0, [priority, [task]]);
	}
	drain() {
		const buckets = this.buckets;
		this.buckets = [];
		return buckets;
	}
};
/**
* Provides a scheduler implementation that batches queued tasks and dispatches them by
* priority.
*
* **When to use**
*
* Use when you need the default runtime scheduler directly, including a
* scheduler that batches queued work by priority and preserves FIFO order within
* each priority.
*
* **Details**
*
* `MixedScheduler` supports synchronous and asynchronous execution modes, uses
* operation counts to decide when fibers should yield, and is the default
* scheduler implementation.
*
* **Gotchas**
*
* On Cloudflare Workers, request-owned scheduler timers can be discarded,
* leaving shared fibers suspended. Use `new MixedScheduler("sync")` for the
* whole application; see `HttpRouter.toWebHandler` for a recipe.
* Sync mode does not yield to the event loop: CPU-heavy work delays other
* requests, and I/O-polling loops can exhaust the CPU limit. It does not fix
* request-owned timers (such as `Effect.sleep`) or request-bound async contexts.
*
* @stability stable
* @category models
* @since 2.0.0
*/
var MixedScheduler = class {
	executionMode;
	setImmediate;
	constructor(executionMode = "async", setImmediateFn) {
		this.executionMode = executionMode;
		this.setImmediate = setImmediateFn ?? (executionMode === "sync" ? setMicrotask : setImmediate);
	}
	/**
	* Returns whether the fiber has reached its operation budget and should yield.
	*
	* **When to use**
	*
	* Use to decide whether a fiber should yield after consuming its current
	* operation budget.
	*
	* @since 2.0.0
	*/
	shouldYield(fiber) {
		return fiber.currentOpCount >= fiber.cache.maxOpsBeforeYield;
	}
	/**
	* Creates a dispatcher that schedules work through this scheduler.
	*
	* **When to use**
	*
	* Use when you need a standalone dispatcher from a scheduler instance, for
	* example in tests that enqueue tasks and then flush them deterministically.
	*
	* @since 4.0.0
	*/
	makeDispatcher() {
		return new MixedSchedulerDispatcher(this.setImmediate);
	}
};
var MixedSchedulerDispatcher = class {
	tasks = /*#__PURE__*/ new PriorityBuckets();
	running = void 0;
	setImmediate;
	constructor(setImmediateFn = setImmediate) {
		this.setImmediate = setImmediateFn;
	}
	/**
	* @since 2.0.0
	*/
	scheduleTask(task, priority) {
		this.tasks.scheduleTask(task, priority);
		if (this.running === void 0) this.running = this.setImmediate(this.afterScheduled);
	}
	/**
	* @since 2.0.0
	*/
	afterScheduled = () => {
		this.running = void 0;
		this.runTasks();
	};
	/**
	* @since 2.0.0
	*/
	runTasks() {
		const buckets = this.tasks.drain();
		for (let i = 0; i < buckets.length; i++) {
			const toRun = buckets[i][1];
			for (let j = 0; j < toRun.length; j++) toRun[j]();
		}
	}
	/**
	* @since 2.0.0
	*/
	flush() {
		while (this.tasks.buckets.length > 0) {
			if (this.running !== void 0) {
				this.running();
				this.running = void 0;
			}
			this.runTasks();
		}
	}
};
/**
* Context reference that controls the maximum number of operations a fiber
* can perform before yielding control back to the scheduler.
*
* **When to use**
*
* Use to tune scheduler fairness for CPU-bound fibers by changing the scheduler
* operation budget that triggers a yield.
*
* **Details**
*
* The default value is `2048` operations, which balances performance and
* fairness by helping prevent long-running fibers from monopolizing the
* execution thread.
*
* @see {@link PreventSchedulerYield} for bypassing scheduler yield checks entirely rather than tuning the operation budget
*
* @stability stable
* @category services
* @since 4.0.0
*/
const MaxOpsBeforeYield = /*#__PURE__*/ Reference("effect/Scheduler/MaxOpsBeforeYield", {
	fiberCached: true,
	defaultValue: () => 2048
});
/**
* Context reference that controls whether the runtime should bypass scheduler
* yield checks. When set to `true`, the fiber run loop won't call
* `Scheduler.shouldYield`.
*
* **When to use**
*
* Use to bypass scheduler yield checks for controlled runtime workloads where
* cooperative yielding should be disabled.
*
* **Gotchas**
*
* Setting this reference to `true` can let long-running fibers monopolize the
* JavaScript thread.
*
* @see {@link MaxOpsBeforeYield} for tuning yield frequency without disabling yield checks
* @see {@link Scheduler} for providing custom scheduler yield behavior
*
* @stability stable
* @category services
* @since 4.0.0
*/
const PreventSchedulerYield = /*#__PURE__*/ Reference("effect/Scheduler/PreventSchedulerYield", {
	fiberCached: true,
	defaultValue: () => false
});
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Tracer.js
/**
* Defines the string key for the parent-span context service.
*
* **When to use**
*
* Use when you need the raw context key for parent span lookup in lower-level
* tracing code.
*
* **Example** (Reading the parent span key)
*
* ```ts import.meta.vitest
* import { Tracer } from "effect"
*
* // The key used to identify parent spans in the context
* Tracer.ParentSpanKey // => "effect/Tracer/ParentSpan"
* ```
*
* @stability stable
* @category constants
* @since 4.0.0
*/
const ParentSpanKey = "effect/Tracer/ParentSpan";
/**
* Defines the string key for the active tracer context reference.
*
* **When to use**
*
* Use when you need the raw context key for active tracer lookup in lower-level
* tracing code.
*
* @stability stable
* @category constants
* @since 4.0.0
*/
const TracerKey = "effect/Tracer";
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/metric.js
/** @internal */
const FiberRuntimeMetricsKey = "effect/Metric/FiberRuntimeMetrics";
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/references.js
/** @internal */
const CurrentStackFrame = /*#__PURE__*/ Reference("effect/References/CurrentStackFrame", {
	fiberCached: true,
	defaultValue: constUndefined
});
/** @internal */
const TracerEnabled = /*#__PURE__*/ Reference("effect/References/TracerEnabled", {
	fiberCached: true,
	defaultValue: constTrue
});
/** @internal */
const CurrentLogLevel = /*#__PURE__*/ Reference("effect/References/CurrentLogLevel", {
	fiberCached: true,
	defaultValue: () => "Info"
});
/** @internal */
const MinimumLogLevel = /*#__PURE__*/ Reference("effect/References/MinimumLogLevel", {
	fiberCached: true,
	defaultValue: () => "Info"
});
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/effect.js
/** @internal */
var Interrupt = class extends ReasonBase {
	constructor(fiberId, annotations = constEmptyAnnotations) {
		super("Interrupt", annotations, "Interrupted");
		this.fiberId = fiberId;
	}
	toString() {
		return `Interrupt(${this.fiberId})`;
	}
	toJSON() {
		return {
			_tag: "Interrupt",
			fiberId: this.fiberId
		};
	}
	[symbol](that) {
		return isInterruptReason(that) && this.fiberId === that.fiberId && this.annotations === that.annotations;
	}
	[symbol$1]() {
		return string$2(`${this._tag}:${this.fiberId}`);
	}
};
/** @internal */
const causeInterrupt = (fiberId) => new CauseImpl([new Interrupt(fiberId)]);
/** @internal */
const hasFails = (self) => self.reasons.some(isFailReason$1);
/** @internal */
const findError = (self) => {
	for (let i = 0; i < self.reasons.length; i++) {
		const reason = self.reasons[i];
		if (reason._tag === "Fail") return succeed$4(reason.error);
	}
	return fail$4(self);
};
/** @internal */
const hasDies = (self) => self.reasons.some(isDieReason);
/**
* Defects take precedence over typed failures when causes are combined: if
* the cause contains a `Die`, every `Fail` becomes a `Die`. `Done` signals
* stay typed so pulls still complete.
*
* @internal
*/
const causeElectDefects = (self) => hasDies(self) && hasFails(self) ? causeFromReasons(self.reasons.map((reason) => reason._tag === "Fail" && !isDone(reason.error) ? new Die(reason.error, reason.annotations) : reason)) : self;
/**
* Selects the first typed error unless a defect is present, in which case
* every `Fail` is elected to `Die`. Interrupts do not block typed recovery.
*
* @internal
*/
const findRecoverableError = (self) => hasDies(self) ? fail$4(causeElectDefects(self)) : findError(self);
/** @internal */
const hasInterrupts = (self) => self.reasons.some(isInterruptReason);
/** @internal */
const causeFilterInterruptors = (self) => {
	let interruptors;
	for (let i = 0; i < self.reasons.length; i++) {
		const f = self.reasons[i];
		if (f._tag !== "Interrupt") continue;
		interruptors ??= /* @__PURE__ */ new Set();
		if (f.fiberId !== void 0) interruptors.add(f.fiberId);
	}
	return interruptors ? succeed$4(interruptors) : fail$4(self);
};
/** @internal */
const causeMap = /*#__PURE__*/ dual(2, (self, f) => {
	let hasFail = false;
	const failures = self.reasons.map((failure) => {
		if (isFailReason$1(failure)) {
			hasFail = true;
			return new Fail(f(failure.error), failure.annotations);
		}
		return failure;
	});
	return hasFail ? causeFromReasons(failures) : self;
});
/** @internal */
const causeSquash = (self) => {
	let die;
	for (const reason of self.reasons) {
		if (reason._tag === "Fail") return reason.error;
		if (reason._tag === "Die") die ??= reason;
	}
	if (die !== void 0) return die.defect;
	if (self.reasons.length > 0) return new globalThis.Error("All fibers interrupted without error");
	return new globalThis.Error("Empty cause");
};
/** @internal */
const FiberTypeId = "~effect/Fiber";
const fiberVariance = {
	_A: identity$1,
	_E: identity$1
};
const fiberIdStore = { id: 0 };
const AsyncResource = /*#__PURE__*/ (() => {
	try {
		return globalThis.process?.getBuiltinModule?.("node:async_hooks")?.AsyncResource;
	} catch {
		return;
	}
})();
const captureAsyncContext = () => AsyncResource === void 0 ? void 0 : new AsyncResource("effect/Fiber");
/** @internal */
const getCurrentFiber = () => globalThis[currentFiberTypeId];
/** @internal */
var FiberImpl = class {
	constructor(context, interruptible = true) {
		this.setContext(context);
		this.id = ++fiberIdStore.id;
		this.currentOpCount = 0;
		this.interruptible = interruptible;
		this._stack = [];
		this._observers = void 0;
		this._exit = void 0;
		this._children = void 0;
		this._interruptedCause = void 0;
		this._yielded = void 0;
		this._running = false;
		this._deferredInterrupt = false;
		this._parent = void 0;
		this._asyncContext = void 0;
		this.cache.runtimeMetrics?.recordFiberStart(this.context);
	}
	get [FiberTypeId]() {
		return fiberVariance;
	}
	get currentDispatcher() {
		return this._dispatcher ??= this.cache.scheduler.makeDispatcher();
	}
	getRef(ref) {
		return get(this.context, ref);
	}
	addObserver(cb) {
		if (this._exit) {
			cb(this._exit);
			return constVoid;
		}
		if (this._observers === void 0) this._observers = [cb];
		else this._observers.push(cb);
		return () => this.removeObserver(cb);
	}
	removeObserver(cb) {
		if (this._exit || this._observers === void 0) return;
		const index = this._observers.indexOf(cb);
		if (index >= 0) this._observers.splice(index, 1);
	}
	interruptUnsafe(fiberId, annotations) {
		if (this._exit) return;
		let cause = causeInterrupt(fiberId);
		if (this.cache.stackFrame) cause = causeAnnotate(cause, make$11(StackTraceKey, this.cache.stackFrame));
		if (annotations) cause = causeAnnotate(cause, annotations);
		this._interruptedCause = this._interruptedCause ? causeAddInterrupts(this._interruptedCause, cause) : cause;
		if (this.interruptible) {
			if (this._running) this._deferredInterrupt = true;
			else this.evaluate(failCause$3(this._interruptedCause));
		}
	}
	pollUnsafe() {
		return this._exit;
	}
	evaluate(effect) {
		if (this._exit) return;
		else if (this._asyncContext !== void 0) {
			const asyncContext = this._asyncContext;
			this._asyncContext = void 0;
			try {
				return asyncContext.runInAsyncScope(this.evaluate, this, effect);
			} finally {
				asyncContext.emitDestroy();
			}
		} else if (this._yielded !== void 0) {
			const yielded = this._yielded;
			this._yielded = void 0;
			yielded();
		}
		const exit = this.runLoop(effect);
		if (exit === Yield) {
			this._asyncContext = captureAsyncContext();
			return;
		}
		const interruptChildren = fiberMiddleware.interruptChildren && fiberMiddleware.interruptChildren(this);
		if (interruptChildren !== void 0) return this.evaluate(flatMap$1(interruptChildren, () => exit));
		this._exit = exit;
		this.cache.runtimeMetrics?.recordFiberEnd(this.context, this._exit);
		if (this._parent) {
			this._parent._children?.delete(this);
			this._parent = void 0;
		}
		if (this._observers !== void 0) {
			const observers = this._observers;
			this._observers = void 0;
			for (let i = 0; i < observers.length; i++) observers[i](exit);
		}
		this._stack.length = 0;
		this._children = void 0;
		this.context = empty();
		const cache = this.cache;
		if (cache.span !== void 0 || cache.stackFrame !== void 0) this.cache = {
			...cache,
			span: void 0,
			stackFrame: void 0
		};
	}
	runLoop(effect) {
		const prevFiber = globalThis[currentFiberTypeId];
		globalThis[currentFiberTypeId] = this;
		const prevRunning = this._running;
		this._running = true;
		let yielding = false;
		let current = effect;
		this.currentOpCount = 0;
		try {
			while (true) try {
				if (this._deferredInterrupt) {
					this._deferredInterrupt = false;
					current = failCause$3(this._interruptedCause);
				}
				this.currentOpCount++;
				const cache = this.cache;
				if (!yielding && !cache.preventYield && cache.scheduler.shouldYield(this)) {
					yielding = true;
					const prev = current;
					current = flatMap$1(yieldNow, () => prev);
				}
				current = cache.tracerContext ? cache.tracerContext(current, this) : current[evaluate](this);
				if (current === Yield) {
					const yielded = this._yielded;
					if ("~effect/Exit" in yielded) {
						this._deferredInterrupt = false;
						this._yielded = void 0;
						return yielded;
					} else if (this._deferredInterrupt) {
						this._yielded = void 0;
						yielded();
						continue;
					}
					return Yield;
				}
			} catch (error) {
				if (!hasProperty(current, "~effect/Effect/evaluate")) return exitDie(`Fiber.runLoop: Not a valid effect: ${String(current)}`);
				current = exitDie(error);
			}
		} finally {
			this._running = prevRunning;
			globalThis[currentFiberTypeId] = prevFiber;
		}
	}
	getCont(symbol) {
		if (this._deferredInterrupt) {
			this._deferredInterrupt = false;
			return deferredInterruptCont;
		}
		while (true) {
			const op = this._stack.pop();
			if (!op) return void 0;
			const all = op[contAll];
			if (all !== void 0) {
				const cont = all.call(op, this);
				if (cont) {
					cont[symbol] = cont;
					return cont;
				}
			}
			if (op[symbol]) return op;
		}
	}
	succeedWith(value) {
		if ((++this.currentOpCount & maxInlineSteps - 1) === 0) return exitSucceed(value);
		const cont = this.getCont(contA);
		return cont ? cont[contA](value, this) : this.yieldWith(exitSucceed(value));
	}
	yieldWith(value) {
		this._yielded = value;
		return Yield;
	}
	children() {
		return this._children ??= /* @__PURE__ */ new Set();
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
	setContext(context) {
		const previous = this.context;
		this.context = context;
		if (previous !== void 0 && hasSameCache(previous, context)) return;
		const root = context.cacheRoot;
		const cache = root._fiberCache ??= makeFiberContextCache(context);
		if (this.cache?.scheduler !== cache.scheduler) this._dispatcher = void 0;
		this.cache = cache;
	}
	get currentSpanLocal() {
		const span = this.cache.span;
		return span?._tag === "Span" ? span : void 0;
	}
};
const makeFiberContextCache = (context) => {
	const currentTracer = getOrUndefinedUnsafe(context, TracerKey);
	return {
		scheduler: get(context, Scheduler),
		tracer: currentTracer,
		tracerContext: currentTracer ? currentTracer["context"] : void 0,
		tracerEnabled: get(context, TracerEnabled),
		span: getOrUndefinedUnsafe(context, ParentSpanKey),
		logLevel: get(context, CurrentLogLevel),
		minimumLogLevel: get(context, MinimumLogLevel),
		stackFrame: get(context, CurrentStackFrame),
		runtimeMetrics: getOrUndefinedUnsafe(context, FiberRuntimeMetricsKey),
		maxOpsBeforeYield: get(context, MaxOpsBeforeYield),
		preventYield: get(context, PreventSchedulerYield)
	};
};
const deferredInterruptCont = {
	[contA](_value, fiber) {
		return failCause$3(fiber._interruptedCause);
	},
	[contE](_cause, fiber) {
		return failCause$3(fiber._interruptedCause);
	}
};
const maxInlineSteps = 32;
const fiberMiddleware = { interruptChildren: void 0 };
const fiberStackAnnotations = (fiber) => {
	if (!fiber.cache.stackFrame) return void 0;
	const annotations = /* @__PURE__ */ new Map();
	annotations.set(InterruptorStackTrace.key, fiber.cache.stackFrame);
	return makeUnsafe$5(annotations);
};
/** @internal */
const fiberAwaitAll = (self) => callback$1((resume) => {
	const iter = self[Symbol.iterator]();
	const exits = [];
	let cancel = void 0;
	function loop() {
		let result = iter.next();
		while (!result.done) {
			if (result.value._exit) {
				exits.push(result.value._exit);
				result = iter.next();
				continue;
			}
			cancel = result.value.addObserver((exit) => {
				exits.push(exit);
				loop();
			});
			return;
		}
		resume(succeed$3(exits));
	}
	loop();
	return sync$1(() => cancel?.());
});
/** @internal */
const fiberInterruptAll = (fibers) => withFiber$1((parent) => {
	const annotations = fiberStackAnnotations(parent);
	let fiberArr = empty$1();
	for (const fiber of fibers) {
		fiber.interruptUnsafe(parent.id, annotations);
		fiberArr.push(fiber);
	}
	return asVoid$1(fiberAwaitAll(fiberArr));
});
/** @internal */
const succeed$3 = exitSucceed;
/** @internal */
const failCause$3 = exitFailCause;
/** @internal */
const fail$3 = exitFail;
/** @internal */
const sync$1 = /*#__PURE__*/ makePrimitive({
	op: "Sync",
	[evaluate](fiber) {
		const thunk = this[args];
		const value = thunk();
		const cont = fiber.getCont(contA);
		return cont ? cont[contA](value, fiber) : fiber.yieldWith(exitSucceed(value));
	}
});
/** @internal */
const suspend$1 = /*#__PURE__*/ makePrimitive({
	op: "Suspend",
	[evaluate](_fiber) {
		const thunk = this[args];
		return thunk();
	}
});
/** @internal */
const yieldNow = /*#__PURE__*/ (/* @__PURE__ */ makePrimitive({
	op: "Yield",
	[evaluate](fiber) {
		let resumed = false;
		fiber.currentDispatcher.scheduleTask(() => {
			if (resumed) return;
			fiber.evaluate(exitVoid);
		}, this["~effect/Effect/args"] ?? 0);
		return fiber.yieldWith(() => {
			resumed = true;
		});
	}
}))(0);
/** @internal */
const succeedNone = /*#__PURE__*/ succeed$3(/*#__PURE__*/ none());
/** @internal */
const failCauseSync$1 = (evaluate) => suspend$1(() => failCause$3(evaluate()));
/** @internal */
const die$2 = (defect) => exitDie(defect);
/** @internal */
const void_$2 = /*#__PURE__*/ succeed$3(void 0);
/** @internal */
const tryPromise$1 = (options) => {
	const f = typeof options === "function" ? options : options.try;
	const catcher = typeof options === "function" ? (cause) => new UnknownError(cause, "An error occurred in Effect.tryPromise") : options.catch;
	return callbackOptions(function(resume, signal) {
		const failWithCatch = (cause) => suspend$1(() => {
			try {
				return fail$3(internalCall(() => catcher(cause)));
			} catch (err) {
				return die$2(err);
			}
		});
		try {
			f(signal).then((a) => resume(succeed$3(a)), (e) => resume(failWithCatch(e)));
		} catch (err) {
			resume(failWithCatch(err));
		}
	}, f.length !== 0);
};
const callbackOptions = /*#__PURE__*/ function() {
	const Proto = /*#__PURE__*/ makePrimitiveProto({
		op: "Async",
		[evaluate](fiber) {
			let resumed = false;
			let yielded = false;
			const controller = this.withSignal ? new AbortController() : void 0;
			const onCancel = this.register.call(fiber.cache.scheduler, (effect) => {
				if (resumed) return;
				resumed = true;
				if (yielded) fiber.evaluate(effect);
				else yielded = effect;
			}, controller?.signal);
			if (yielded !== false) return yielded;
			yielded = true;
			fiber.yieldWith(() => {
				resumed = true;
			});
			if (controller === void 0 && onCancel === void 0) return Yield;
			fiber._stack.push(asyncFinalizer(() => {
				resumed = true;
				controller?.abort();
				return onCancel ?? exitVoid;
			}));
			return Yield;
		}
	});
	const AsyncImpl = function(register, withSignal) {
		this.register = register;
		this.withSignal = withSignal;
	};
	AsyncImpl.prototype = Proto;
	return function(register, withSignal) {
		return new AsyncImpl(register, withSignal);
	};
}();
const asyncFinalizer = /*#__PURE__*/ makePrimitive({
	op: "AsyncFinalizer",
	[contAll](fiber) {
		if (fiber.interruptible) {
			fiber.interruptible = false;
			fiber._stack.push(setInterruptibleTrue);
		}
	},
	[contE](cause, _fiber) {
		return hasInterrupts(cause) ? flatMap$1(combineFinalizerCause(exitFailCause(cause), this[args]()), () => failCause$3(cause)) : failCause$3(cause);
	}
});
/** @internal */
const callback$1 = (register) => callbackOptions(register, register.length >= 2);
const fnBody = (bodyOrOptions, pipeables) => typeof bodyOrOptions === "function" ? bodyOrOptions : pipeables.shift().bind(bodyOrOptions.self);
/** @internal */
const fnUntraced = (bodyOrOptions, ...pipeables) => {
	const body = fnBody(bodyOrOptions, pipeables);
	const fn = pipeables.length === 0 ? function() {
		return suspend$1(() => fromIteratorUnsafe(body.apply(this, arguments)));
	} : function() {
		let effect = suspend$1(() => fromIteratorUnsafe(body.apply(this, arguments)));
		for (let i = 0; i < pipeables.length; i++) effect = pipeables[i](effect, ...arguments);
		return effect;
	};
	return defineFunctionLength(body.length, fn);
};
const defineFunctionLength = (length, fn) => Object.defineProperty(fn, "length", {
	value: length,
	configurable: true
});
/** @internal */
const fnUntracedEager$1 = (bodyOrOptions, ...pipeables) => {
	const body = fnBody(bodyOrOptions, pipeables);
	return defineFunctionLength(body.length, pipeables.length === 0 ? function() {
		return fromIteratorEagerUnsafe(() => body.apply(this, arguments));
	} : function() {
		let effect = fromIteratorEagerUnsafe(() => body.apply(this, arguments));
		for (const pipeable of pipeables) effect = pipeable(effect, ...arguments);
		return effect;
	});
};
const fromIteratorEagerUnsafe = (evaluate) => {
	try {
		const iterator = evaluate();
		let value = void 0;
		while (true) {
			const state = iterator.next(value);
			if (state.done) return succeed$3(state.value);
			const primitive = state.value;
			if (primitive && primitive._tag === "Success") {
				value = primitive.value;
				continue;
			} else if (primitive && primitive._tag === "Failure") return state.value;
			else {
				let isFirstExecution = true;
				return suspend$1(() => {
					if (isFirstExecution) {
						isFirstExecution = false;
						return flatMap$1(state.value, (value) => fromIteratorUnsafe(iterator, value));
					} else return suspend$1(() => fromIteratorUnsafe(evaluate()));
				});
			}
		}
	} catch (error) {
		return die$2(error);
	}
};
const fromIteratorUnsafe = /*#__PURE__*/ function() {
	const Proto = /*#__PURE__*/ makePrimitiveProto({
		op: "Iterator",
		[contA](value, fiber) {
			const iter = this.iterator;
			while (true) {
				const state = iter.next(value);
				if (state.done) return succeed$3(state.value);
				if (!effectIsExit(state.value)) {
					fiber._stack.push(this);
					return state.value;
				} else if (state.value._tag === "Failure") return state.value;
				value = state.value.value;
			}
		},
		[evaluate](fiber) {
			return this[contA](this.initial, fiber);
		}
	});
	const IteratorImpl = function(iterator, initial) {
		this.iterator = iterator;
		this.initial = initial;
	};
	IteratorImpl.prototype = Proto;
	return function(iterator, initial) {
		return new IteratorImpl(iterator, initial);
	};
}();
/** @internal */
const as$1 = /*#__PURE__*/ dual(2, (self, value) => new ContImpl(self, returnPayload, succeed$3(value)));
const evaluateCont = function(fiber) {
	fiber._stack.push(this);
	return this[args];
};
const OnSuccessProto = /*#__PURE__*/ makePrimitiveProto({
	op: "OnSuccess",
	[evaluate]: evaluateCont
});
const ContImpl = function(self, cont, payload) {
	this[args] = self;
	this[contA] = cont;
	this.payload = payload;
};
ContImpl.prototype = OnSuccessProto;
const returnPayload = function() {
	return this.payload;
};
const succeedPayload = function(_value, fiber) {
	return fiber.succeedWith(this.payload);
};
const continuationMarksStack = /*#__PURE__*/ (() => {
	const limit = /*#__PURE__*/ getStackTraceLimit();
	if (limit === 0) return false;
	const marker = "~effect/Effect/stackProbe";
	const probe = { [marker]: function stackProbe() {
		return /* @__PURE__ */ new Error();
	} };
	setStackTraceLimit(1);
	const error = /*#__PURE__*/ probe[marker]();
	setStackTraceLimit(limit);
	return error.stack?.includes("[as " + marker + "]") === true;
})();
const mapCont = function(value, fiber) {
	const f = this.payload;
	return fiber.succeedWith(continuationMarksStack ? f(value) : internalCall(() => f(value)));
};
const andThenCont = function(value) {
	const f = this.payload;
	return f(value);
};
const tapCont = function(value) {
	const f = this.payload;
	return new ContImpl(f(value), succeedPayload, value);
};
const tapEffectCont = function(value) {
	return new ContImpl(this.payload, succeedPayload, value);
};
/** @internal */
const asSome = (self) => map$2(self, some);
/** @internal */
const andThen$1 = /*#__PURE__*/ dual(2, (self, f) => new ContImpl(self, isEffect(f) ? returnPayload : andThenCont, f));
/** @internal */
const tap$1 = /*#__PURE__*/ dual(2, (self, f) => new ContImpl(self, isEffect(f) ? tapEffectCont : tapCont, f));
/** @internal */
const asVoid$1 = (self) => new ContImpl(self, returnPayload, exitVoid);
/** @internal */
const flatMap$1 = /*#__PURE__*/ dual(2, (self, f) => new ContImpl(self, andThenCont, f));
/** @internal */
const effectIsExit = (effect) => effect[ExitTypeId] !== void 0;
/** @internal */
const flatMapEager$1 = /*#__PURE__*/ dual(2, (self, f) => {
	if (effectIsExit(self)) return self._tag === "Success" ? f(self.value) : self;
	return flatMap$1(self, f);
});
/** @internal */
const map$2 = /*#__PURE__*/ dual(2, (self, f) => new ContImpl(self, mapCont, f));
/** @internal */
const mapEager$1 = /*#__PURE__*/ dual(2, (self, f) => effectIsExit(self) ? exitMap(self, f) : map$2(self, f));
/** @internal */
const exitInterrupt = (fiberId) => exitFailCause(causeInterrupt(fiberId));
/** @internal */
const exitIsSuccess = (self) => self._tag === "Success";
/** @internal */
const exitVoid = /*#__PURE__*/ exitSucceed(void 0);
/** @internal */
const exitMap = /*#__PURE__*/ dual(2, (self, f) => self._tag === "Success" ? exitSucceed(f(self.value)) : self);
/** @internal */
const exitAsVoidAll = (exits) => {
	const failures = [];
	for (const exit of exits) if (exit._tag === "Failure") failures.push(...exit.cause.reasons);
	return failures.length === 0 ? exitVoid : exitFailCause(causeFromReasons(failures));
};
/** @internal */
const updateContext = /*#__PURE__*/ dual(2, (self, f) => withFiber$1((fiber) => {
	const prevContext = fiber.context;
	const nextContext = f(prevContext);
	if (prevContext === nextContext) return self;
	fiber.setContext(nextContext);
	onExitUnsafe(fiber, () => {
		fiber.setContext(prevContext);
	});
	return self;
}));
/** @internal */
const contextWith$1 = (f) => withFiber$1((fiber) => f(fiber.context));
/** @internal */
const provideContext = /*#__PURE__*/ dual(2, (self, context) => {
	if (effectIsExit(self)) return self;
	return updateContext(self, merge$1(context));
});
/** @internal */
const provideService = function() {
	if (arguments.length === 1) return dual(2, (self, impl) => provideServiceImpl(self, arguments[0], impl));
	return dual(3, (self, service, impl) => provideServiceImpl(self, service, impl)).apply(this, arguments);
};
const provideServiceImpl = (self, service, implementation) => updateContext(self, add(service, implementation));
/** @internal */
const when$1 = /*#__PURE__*/ dual(2, (self, condition) => flatMap$1(condition, (pass) => pass ? asSome(self) : succeedNone));
/** @internal */
const catchCause$1 = /*#__PURE__*/ dual(2, (self, f) => new OnFailureImpl(self, f.length !== 1 ? (cause) => f(cause) : f));
const OnFailureProto = /*#__PURE__*/ makePrimitiveProto({
	op: "OnFailure",
	[evaluate]: evaluateCont
});
const OnFailureImpl = function(self, f) {
	this[args] = self;
	this[contE] = f;
};
OnFailureImpl.prototype = OnFailureProto;
/** @internal */
const catchCauseFilter = /*#__PURE__*/ dual(3, (self, filter, f) => catchCause$1(self, (cause) => {
	const eb = filter(cause);
	return isFailure(eb) ? failCause$3(eb.failure) : f(eb.success, cause);
}));
/** @internal */
const catch_$1 = /*#__PURE__*/ dual(2, (self, f) => catchCauseFilter(self, findRecoverableError, (e) => f(e)));
const OnSuccessAndFailureProto = /*#__PURE__*/ makePrimitiveProto({
	op: "OnSuccessAndFailure",
	[evaluate]: evaluateCont
});
const OnSuccessAndFailureImpl = function(self, onSuccess, onFailure) {
	this[args] = self;
	this[contA] = onSuccess;
	this[contE] = onFailure;
};
OnSuccessAndFailureImpl.prototype = OnSuccessAndFailureProto;
/** @internal */
const exit$1 = (self) => effectIsExit(self) ? exitSucceed(self) : exitPrimitive(self);
const exitPrimitive = /*#__PURE__*/ makePrimitive({
	op: "Exit",
	[evaluate](fiber) {
		fiber._stack.push(this);
		return this[args];
	},
	[contA](value, fiber, exit) {
		return fiber.succeedWith(exit ?? exitSucceed(value));
	},
	[contE](cause, fiber, exit) {
		return fiber.succeedWith(exit ?? exitFailCause(cause));
	}
});
/** @internal */
const ScopeTypeId = "~effect/Scope";
/** @internal */
const ScopeCloseableTypeId = "~effect/Scope/Closeable";
/** @internal */
const scopeTag = /*#__PURE__*/ Service("effect/Scope");
/** @internal */
const scopeClose = (self, exit_) => withFiber$1((fiber) => {
	fiberEnterUninterruptibleUnsafe(fiber);
	return scopeCloseUnsafe(self, exit_) ?? void_$2;
});
/** @internal */
const scopeCloseUnsafe = (self, exit_) => {
	const state = self.state;
	if (state._tag === "Closed") return;
	self.state = {
		_tag: "Closed",
		exit: exit_
	};
	if (self.parent !== void 0) scopeRemoveFinalizerUnsafe(self.parent, self);
	if (state._tag === "Empty") return;
	if (state.finalizer !== void 0) return runFinalizer(state.finalizer, exit_);
	const finalizers = state.finalizers;
	if (finalizers.size === 1) return runFinalizer(finalizers.values().next().value, exit_);
	return scopeCloseFinalizers(self, finalizers, exit_);
};
const runFinalizer = (finalizer, exit_) => {
	try {
		return finalizer(exit_);
	} catch (defect) {
		return exitDie(defect);
	}
};
const combineFinalizerCause = (exit_, finalizer) => exitIsSuccess(exit_) ? finalizer : catchCause$1(finalizer, (cause) => failCause$3(causeElectDefects(causeCombine(exit_.cause, cause))));
const scopeCloseFinalizers = /*#__PURE__*/ fnUntraced(function* (self, finalizers, exit_) {
	let exits = [];
	const fibers = [];
	const arr = Array.from(finalizers);
	const parent = getCurrentFiber();
	for (let i = arr.length - 1; i >= 0; i--) {
		const [key, finalizer] = arr[i];
		if (self.strategy === "sequential") exits.push(yield* exit$1(runFinalizer(finalizer, exit_)));
		else if (key !== parent) fibers.push(forkUnsafe$1(parent, runFinalizer(finalizer, exit_), true, true, "inherit"));
	}
	if (fibers.length > 0) exits = yield* fiberAwaitAll(fibers);
	return yield* exitAsVoidAll(exits);
});
/** @internal */
const scopeForkUnsafe = (scope, finalizerStrategy) => {
	const child = makeScope(finalizerStrategy, scope);
	if (scope.state._tag === "Closed") {
		child.state = scope.state;
		return child;
	}
	scopeAddFinalizerUnsafe(scope, child, (exit) => scopeClose(child, exit));
	return child;
};
/** @internal */
const scopeAddFinalizerExit = (scope, finalizer) => {
	return suspend$1(() => {
		if (scope.state._tag === "Closed") return finalizer(scope.state.exit);
		scopeAddFinalizerUnsafe(scope, {}, finalizer);
		return void_$2;
	});
};
/** @internal */
const scopeAddFinalizer = (scope, finalizer) => scopeAddFinalizerExit(scope, constant(finalizer));
/** @internal */
const scopeAddFinalizerUnsafe = (scope, key, finalizer) => {
	if (scope.state._tag === "Empty") scope.state = {
		_tag: "Open",
		finalizerKey: key,
		finalizer,
		finalizers: void 0
	};
	else if (scope.state._tag === "Open") {
		const state = scope.state;
		if (state.finalizer !== void 0) {
			state.finalizers = /* @__PURE__ */ new Map([[state.finalizerKey, state.finalizer]]);
			state.finalizerKey = void 0;
			state.finalizer = void 0;
			state.finalizers.set(key, finalizer);
		} else state.finalizers.set(key, finalizer);
	}
};
/** @internal */
const scopeRemoveFinalizerUnsafe = (scope, key) => {
	if (scope.state._tag !== "Open") return;
	const state = scope.state;
	if (state.finalizerKey === key) scope.state = constScopeEmpty;
	else if (state.finalizers !== void 0) {
		state.finalizers.delete(key);
		if (state.finalizers.size === 0) scope.state = constScopeEmpty;
	}
};
/** @internal */
const scopeMakeUnsafe = (finalizerStrategy) => makeScope(finalizerStrategy, void 0);
const makeScope = (finalizerStrategy = "sequential", parent) => ({
	[ScopeCloseableTypeId]: ScopeCloseableTypeId,
	[ScopeTypeId]: ScopeTypeId,
	strategy: finalizerStrategy,
	parent,
	state: constScopeEmpty
});
const constScopeEmpty = { _tag: "Empty" };
/** @internal */
const scope = scopeTag;
/** @internal */
const provideScope = /*#__PURE__*/ provideService(scopeTag);
/** @internal */
const addFinalizer$2 = (finalizer) => flatMap$1(scope, (scope) => contextWith$1((context) => scopeAddFinalizerExit(scope, (exit) => provideContext(finalizer(exit), context))));
const OnExitImpl = /*#__PURE__*/ function() {
	const Proto = /*#__PURE__*/ makePrimitiveProto({
		op: "OnExit",
		[evaluate](fiber) {
			fiber._stack.push(this);
			return this.effect;
		},
		[contAll](fiber) {
			if (fiber.interruptible && this.interruptible !== true) {
				fiber._stack.push(setInterruptibleTrue);
				fiber.interruptible = false;
			}
		},
		[contA](value, _, exit) {
			exit ??= exitSucceed(value);
			let eff;
			try {
				eff = this.onExit(exit);
			} catch (defect) {
				eff = exitDie(defect);
			}
			return eff ? flatMap$1(eff, (_) => exit) : exit;
		},
		[contE](cause, _, exit) {
			exit ??= exitFailCause(cause);
			let eff;
			try {
				eff = this.onExit(exit);
			} catch (defect) {
				eff = exitDie(defect);
			}
			return eff ? flatMap$1(combineFinalizerCause(exit, eff), (_) => exit) : exit;
		}
	});
	const OnExitImpl = function(effect, onExit, interruptible) {
		this.effect = effect;
		this.onExit = onExit;
		this.interruptible = interruptible;
	};
	OnExitImpl.prototype = Proto;
	return OnExitImpl;
}();
/** @internal */
const onExitPrimitive = (effect, onExit, interruptible) => new OnExitImpl(effect, onExit, interruptible);
/**
* Pushes an exit finalizer for the current `withFiber` evaluation. Only call inside `withFiber`.
* @internal
*/
const onExitUnsafe = (fiber, f) => {
	fiber._stack.push(new OnExitImpl(void 0, f, void 0));
};
/** @internal */
const onExit = /*#__PURE__*/ dual(2, onExitPrimitive);
/** @internal */
const onErrorFilter = /*#__PURE__*/ dual(3, (self, filter, f) => onExit(self, (exit) => {
	if (exit._tag !== "Failure") return void_$2;
	const result = filter(exit.cause);
	return isFailure(result) ? void_$2 : f(result.success, exit.cause);
}));
/** @internal */
const onInterrupt$1 = /*#__PURE__*/ dual(2, (self, finalizer) => onErrorFilter(causeFilterInterruptors, finalizer)(self));
/** @internal */
const interrupt$2 = /*#__PURE__*/ withFiber$1((fiber) => failCause$3(causeInterrupt(fiber.id)));
/** @internal */
const uninterruptible$1 = (self) => withFiber$1((fiber) => {
	if (!fiber.interruptible) return self;
	fiber.interruptible = false;
	fiber._stack.push(setInterruptibleTrue);
	return self;
});
const setInterruptibleTrue = /*#__PURE__*/ (/* @__PURE__ */ makePrimitive({
	op: "SetInterruptible",
	[contAll](fiber) {
		fiber.interruptible = this[args];
		if (fiber._interruptedCause && fiber.interruptible) return () => failCause$3(fiber._interruptedCause);
	}
}))(true);
/**
* Makes the current fiber uninterruptible for the returned effect without an
* extra primitive. Call only within `withFiber`.
*
* @internal
*/
const fiberEnterUninterruptibleUnsafe = (fiber) => {
	const impl = fiber;
	if (!impl.interruptible) return;
	impl.interruptible = false;
	impl._stack.push(setInterruptibleTrue);
};
/** @internal */
const whileLoop = /*#__PURE__*/ makePrimitive({
	op: "While",
	[contA](value, fiber) {
		this[args].step(value);
		if (this["~effect/Effect/args"].while()) {
			fiber._stack.push(this);
			return this[args].body();
		}
		return exitVoid;
	},
	[evaluate](fiber) {
		if (this["~effect/Effect/args"].while()) {
			fiber._stack.push(this);
			return this[args].body();
		}
		return exitVoid;
	}
});
/** @internal */
const forEach$1 = /*#__PURE__*/ dual((args) => typeof args[1] === "function", (iterable, f, options) => suspend$1(() => {
	const concurrency = resolveConcurrency(options?.concurrency);
	if (concurrency === 1) return forEachSequential(iterable, f, options);
	const items = fromIterable$1(iterable);
	let length = items.length;
	if (length === 0) return options?.discard ? void_$2 : succeed$3([]);
	const out = options?.discard ? void 0 : new Array(length);
	const eff = forEachConcurrent({
		f,
		out
	}, items, { concurrency });
	return eff ? as$1(eff, out) : succeed$3(out);
}));
const forEachSequential = (iterable, f, options) => suspend$1(() => {
	const out = options?.discard ? void 0 : [];
	const iterator = iterable[Symbol.iterator]();
	let state = iterator.next();
	let index = 0;
	return as$1(whileLoop({
		while: () => !state.done,
		body: () => f(state.value, index++),
		step: (b) => {
			if (out) out.push(b);
			state = iterator.next();
		}
	}), out);
});
/** @internal */
const resolveConcurrency = (concurrency) => concurrency === "unbounded" ? Number.POSITIVE_INFINITY : Math.max(1, concurrency ?? 1);
/** @internal */
const iterateEager = () => (options) => {
	const onItem = options.onItem;
	const step = options.step;
	const resumeSequential = (state, items, index, end, effect) => flatMap$1(exit$1(effect), (itemExit) => step(state, items[index], itemExit, index) ?? runSequential(state, items, index + 1, end) ?? void_$2);
	const runSequential = (state, items, index = 0, end = items.length) => {
		for (; index < end; index++) {
			const item = items[index];
			const effect = onItem(state, item, index);
			if (!effectIsExit(effect)) return resumeSequential(state, items, index, end, effect);
			const terminal = step(state, item, effect, index);
			if (terminal) return terminal._tag === "Failure" ? terminal : void 0;
		}
	};
	return runSequential;
};
const iterateConcurrentImpl = (options) => {
	const onItem = options.onItem;
	const step = options.step;
	return (state, items, opts) => {
		let index = 0;
		const end = opts.end ?? items.length;
		const concurrency = opts.concurrency;
		let done = false;
		let parentFiber;
		let fibers;
		let resume;
		let terminal;
		let effect;
		const failDefect = (error) => {
			const defect = exitDie(error);
			terminal = defect;
			done = true;
			return fibers && fibers.size > 0 ? flatMap$1(uninterruptible$1(fiberInterruptAll(Array.from(fibers))), () => terminal ?? defect) : defect;
		};
		const go = () => {
			let paused = false;
			for (; !terminal && index < end; index++) {
				const item = items[index];
				const eff = effect ?? onItem(state, item, index);
				if (effectIsExit(eff)) {
					terminal = step(state, item, eff, index);
					if (terminal) break;
				} else if (!parentFiber) return callback$1((cb) => {
					parentFiber = getCurrentFiber();
					fibers = /* @__PURE__ */ new Set();
					effect = eff;
					resume = cb;
					let result;
					try {
						result = go();
					} catch (error) {
						return cb(failDefect(error));
					}
					if (result) return cb(result);
					return suspend$1(() => {
						terminal ??= exitVoid;
						return flatMap$1(fibers ? fiberInterruptAll(fibers) : void_$2, () => terminal?._tag === "Failure" ? terminal : void_$2);
					});
				});
				else {
					effect = void 0;
					const fiber = forkUnsafe$1(parentFiber, eff, true, true, "inherit");
					if (fiber._exit) {
						terminal = step(state, item, fiber._exit, index);
						if (terminal) break;
						continue;
					}
					fibers.add(fiber);
					const currentIndex = index;
					fiber.addObserver((exit) => {
						fibers.delete(fiber);
						try {
							if (terminal) {
								if (exit._tag === "Failure") {
									const reasons = exit.cause.reasons.filter((reason) => reason._tag !== "Interrupt");
									if (reasons.length > 0) {
										const cause = causeFromReasons(reasons);
										terminal = exitFailCause(terminal._tag === "Failure" ? causeElectDefects(causeCombine(terminal.cause, cause)) : cause);
									}
								}
							} else {
								const result = step(state, item, exit, currentIndex);
								if (result) {
									terminal = result;
									go();
								}
							}
							if (paused) {
								const eff = go();
								if (eff) resume(eff);
							} else if (done && fibers.size === 0) resume(terminal ?? void_$2);
						} catch (error) {
							resume(failDefect(error));
						}
					});
					if (fibers.size < concurrency) continue;
					paused = true;
					index++;
					return;
				}
			}
			done = true;
			if (terminal) {
				if (fibers && fibers.size > 0) {
					const annotations = fiberStackAnnotations(parentFiber);
					fibers.forEach((f) => f.interruptUnsafe(parentFiber.id, annotations));
					return;
				}
				if (resume || terminal._tag === "Failure") return terminal;
			} else if (resume) {
				if (!fibers) return exitVoid;
				else if (fibers.size === 0) resume(void_$2);
			}
		};
		return go();
	};
};
/** @internal */
const iterateConcurrent = () => (options) => iterateConcurrentImpl(options);
const forEachConcurrent = /*#__PURE__*/ iterateConcurrentImpl({
	onItem(state, item, index) {
		return state.f(item, index);
	},
	step(state, _, exit, index) {
		if (exit._tag === "Failure") return exit;
		else if (state.out) state.out[index] = exit.value;
	}
});
/** @internal */
const forkUnsafe$1 = (parent, effect, immediate = false, daemon = false, uninterruptible = false) => {
	const parentRuntime = parent;
	const interruptible = uninterruptible === "inherit" ? parentRuntime.interruptible : !uninterruptible;
	const child = new FiberImpl(parentRuntime.context, interruptible);
	if (!daemon) {
		parentRuntime.children().add(child);
		child._parent = parentRuntime;
	}
	if (immediate) child.evaluate(effect);
	else {
		child._asyncContext = captureAsyncContext();
		parentRuntime.currentDispatcher.scheduleTask(() => child.evaluate(effect), 0);
	}
	return child;
};
/** @internal */
const runForkWith = (context) => (effect, options) => {
	const fiber = new FiberImpl(options?.scheduler ? add(context, Scheduler, options.scheduler) : context, options?.uninterruptible !== true);
	fiber.evaluate(effect);
	if (fiber._exit) return fiber;
	if (options?.signal) {
		if (options.signal.aborted) fiber.interruptUnsafe();
		else {
			const abort = () => fiber.interruptUnsafe();
			options.signal.addEventListener("abort", abort, { once: true });
			fiber.addObserver(() => options.signal.removeEventListener("abort", abort));
		}
	}
	if (options?.onFiberStart) options.onFiberStart(fiber);
	return fiber;
};
/** @internal */
const runPromiseExitWith = (context) => {
	const runFork = runForkWith(context);
	return (effect, options) => {
		const fiber = runFork(effect, options);
		return new Promise((resolve) => {
			fiber.addObserver((exit) => resolve(exit));
		});
	};
};
/** @internal */
const runPromiseWith = (context) => {
	const runPromiseExit = runPromiseExitWith(context);
	return (effect, options) => runPromiseExit(effect, options).then((exit) => {
		if (exit._tag === "Failure") throw causeSquash(exit.cause);
		return exit.value;
	});
};
/** @internal */
const runPromise$1 = /*#__PURE__*/ runPromiseWith(/*#__PURE__*/ empty());
/** @internal */
const runSyncExitWith = (context) => {
	const runFork = runForkWith(context);
	return (effect) => {
		if (effectIsExit(effect)) return effect;
		const scheduler = new MixedScheduler("sync");
		const fiber = runFork(effect, { scheduler });
		fiber._dispatcher?.flush();
		return fiber._exit ?? exitDie(new AsyncFiberError(fiber));
	};
};
/** @internal */
const runSyncExit$1 = /*#__PURE__*/ runSyncExitWith(/*#__PURE__*/ empty());
const succeedTrue = /*#__PURE__*/ succeed$3(true);
const succeedFalse = /*#__PURE__*/ succeed$3(false);
var Latch = class {
	waiters = [];
	scheduled = void 0;
	_isOpen;
	constructor(isOpen) {
		this._isOpen = isOpen;
	}
	scheduleUnsafe(fiber) {
		if (this.waiters.length === 0) return succeedTrue;
		if (this.scheduled === void 0) {
			this.scheduled = this.waiters;
			fiber.currentDispatcher.scheduleTask(this.flushScheduled, 0);
		} else for (let i = 0; i < this.waiters.length; i++) this.scheduled.push(this.waiters[i]);
		this.waiters = [];
		return succeedTrue;
	}
	flushScheduled = () => {
		if (this.scheduled === void 0) return;
		const waiters = this.scheduled;
		this.scheduled = void 0;
		for (let i = 0; i < waiters.length; i++) waiters[i](exitVoid);
	};
	flushWaiters() {
		const waiters = this.waiters;
		this.waiters = [];
		this.flushScheduled();
		for (let i = 0; i < waiters.length; i++) waiters[i](exitVoid);
	}
	open = /*#__PURE__*/ withFiber$1((fiber) => {
		if (this._isOpen) return succeedFalse;
		this._isOpen = true;
		return this.scheduleUnsafe(fiber);
	});
	release = /*#__PURE__*/ withFiber$1((fiber) => this._isOpen ? succeedFalse : this.scheduleUnsafe(fiber));
	openUnsafe() {
		if (this._isOpen) return false;
		this._isOpen = true;
		this.flushWaiters();
		return true;
	}
	await = /*#__PURE__*/ callback$1((resume) => {
		if (this._isOpen) return resume(void_$2);
		this.waiters.push(resume);
		return sync$1(() => {
			let index = this.waiters.indexOf(resume);
			if (index !== -1) this.waiters.splice(index, 1);
			else if (this.scheduled !== void 0) {
				index = this.scheduled.indexOf(resume);
				if (index !== -1) this.scheduled.splice(index, 1);
			}
		});
	});
	closeUnsafe() {
		if (!this._isOpen) return false;
		this._isOpen = false;
		return true;
	}
	close = /*#__PURE__*/ sync$1(() => this.closeUnsafe());
	whenOpen = (self) => flatMap$1(this.await, () => self);
	isOpen() {
		return this._isOpen;
	}
};
/** @internal */
const makeLatchUnsafe = (open) => new Latch(open ?? false);
/** @internal */
const AsyncFiberErrorTypeId = "~effect/Cause/AsyncFiberError";
/** @internal */
var AsyncFiberError = class extends (/*#__PURE__*/ TaggedError$2("AsyncFiberError")) {
	[AsyncFiberErrorTypeId] = AsyncFiberErrorTypeId;
	constructor(fiber) {
		super({
			message: "An asynchronous Effect was executed with Effect.runSync",
			fiber
		});
	}
};
/** @internal */
const UnknownErrorTypeId = "~effect/Cause/UnknownError";
/** @internal */
var UnknownError = class extends (/*#__PURE__*/ TaggedError$2("UnknownError")) {
	[UnknownErrorTypeId] = UnknownErrorTypeId;
	constructor(cause, message) {
		super({
			message,
			cause
		});
	}
};
const colors = {
	bold: "1",
	red: "31",
	green: "32",
	yellow: "33",
	blue: "34",
	cyan: "36",
	white: "37",
	gray: "90",
	black: "30",
	bgBrightRed: "101"
};
colors.gray, colors.blue, colors.green, colors.yellow, colors.red, colors.bgBrightRed, colors.black;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Cause.js
/**
* Narrows a `Reason` to `Fail`.
*
* **When to use**
*
* Use as a predicate for `Array.filter` to pick out typed `Fail` reasons when
* iterating over `cause.reasons`.
*
* **Example** (Filtering fail reasons)
*
* ```ts import.meta.vitest
* import { Cause } from "effect"
*
* const cause = Cause.fail("error")
* const fails = cause.reasons.filter(Cause.isFailReason)
* fails[0].error // => "error"
* ```
*
* @see {@link isDieReason} — narrow to `Die`
* @see {@link isInterruptReason} — narrow to `Interrupt`
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isFailReason = isFailReason$1;
/**
* Creates a `Cause` containing a single `Die` reason with the
* given defect.
*
* **When to use**
*
* Use to construct a cause from an untyped defect or unexpected thrown value.
*
* **Example** (Creating a die cause)
*
* ```ts import.meta.vitest
* import { Cause } from "effect"
*
* Cause.die("Unexpected") // => Cause.fromReasons([Cause.makeDieReason("Unexpected")])
* ```
*
* @see {@link fail} — for typed errors
* @see {@link interrupt} — for fiber interruptions
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const die$1 = causeDie;
/**
* Transforms the typed error values inside a `Cause` using the
* provided function. Only `Fail` reasons are affected; `Die` and `Interrupt`
* reasons pass through unchanged.
*
* **When to use**
*
* Use to transform expected typed failures while preserving defects and
* interruptions unchanged.
*
* **Details**
*
* If at least one `Fail` reason exists, this returns a new `Cause`
* containing the mapped failures. If the cause has no `Fail` reasons, the
* original cause is returned unchanged.
*
* **Example** (Mapping errors to uppercase)
*
* ```ts import.meta.vitest
* import { Cause } from "effect"
*
* const cause = Cause.fail("error")
* const mapped = Cause.map(cause, (e) => e.toUpperCase())
* const reason = mapped.reasons[0]
* if (Cause.isFailReason(reason)) {
*   reason.error // => "ERROR"
* }
* ```
*
* @stability stable
* @category mapping
* @since 2.0.0
*/
const map$1 = causeMap;
/**
* Creates an Effect that fails with a `Done` error. Shorthand for
* `Effect.fail(Cause.Done(value))`.
*
* **When to use**
*
* Use when you model stream or queue completion through the error channel.
*
* **Example** (Failing with Done)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Exit } from "effect"
*
* const program = Cause.done("finished")
*
* await Effect.runPromiseExit(program) // => Exit.fail(Cause.Done("finished"))
* ```
*
* @see {@link Done} — create the signal value without an Effect
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const done$1 = done$2;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Exit.js
/**
* Creates a successful Exit containing the given value.
*
* **When to use**
*
* Use when you need an Exit that contains a known success value.
*
* **Details**
*
* Returns a `Success<A>` with the provided value. Does not perform any
* computation.
*
* **Example** (Creating a successful Exit)
*
* ```ts import.meta.vitest
* import { Exit } from "effect"
*
* Exit.succeed(42) // => Exit.succeed(42)
* ```
*
* @see {@link fail} to create a failed Exit
* @see {@link void_ void} for a pre-allocated success with no value
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const succeed$2 = exitSucceed;
/**
* Creates a failed Exit from a Cause.
*
* **When to use**
*
* Use when you already have a `Cause<E>` and want to wrap it in an Exit
* for advanced error handling where you need full control over the Cause
* structure.
*
* **Details**
*
* Returns a `Failure<never, E>`. If you only have an error value, use
* {@link fail} instead.
*
* **Example** (Creating a failed Exit from a Cause)
*
* ```ts import.meta.vitest
* import { Cause, Exit } from "effect"
*
* Exit.failCause(Cause.fail("Something went wrong")) // => Exit.fail("Something went wrong")
* ```
*
* @see {@link fail} to create a Failure from a plain error value
* @see {@link die} to create a Failure from a defect
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const failCause$2 = exitFailCause;
/**
* Creates a failed Exit from a typed error value.
*
* **When to use**
*
* Use when you need to represent an expected typed failure as an `Exit`.
*
* **Details**
*
* The error is wrapped in a `Cause.Fail` internally.
*
* Returns a `Failure<never, E>`.
*
* **Example** (Creating a failed Exit)
*
* ```ts import.meta.vitest
* import { Exit } from "effect"
*
* Exit.fail("Something went wrong") // => Exit.fail("Something went wrong")
* ```
*
* @see {@link succeed} to create a successful Exit
* @see {@link die} to create a Failure from an unexpected defect
* @see {@link failCause} to create a Failure from a full Cause
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fail$2 = exitFail;
/**
* Creates a failed Exit representing fiber interruption.
*
* **When to use**
*
* Use to signal that a fiber was interrupted.
*
* **Details**
*
* Optionally pass a fiber ID to identify which fiber was interrupted. Returns
* a `Failure<never>` with an `Interrupt` cause.
*
* **Example** (Creating an interruption Exit)
*
* ```ts import.meta.vitest
* import { Exit } from "effect"
*
* Exit.interrupt(123) // => Exit.interrupt(123)
* ```
*
* @see {@link hasInterrupts} to check whether an Exit contains interruptions
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const interrupt$1 = exitInterrupt;
const void_$1 = exitVoid;
/**
* Checks whether an Exit is a Success.
*
* **When to use**
*
* Use as a type guard to narrow `Exit<A, E>` to `Success<A, E>` and access the
* `value` property.
*
* **Example** (Narrowing to success)
*
* ```ts import.meta.vitest
* import { Exit } from "effect"
*
* const exit = Exit.succeed(42)
*
* if (Exit.isSuccess(exit)) {
*   exit.value // => 42
* }
* ```
*
* @see {@link isFailure} for the opposite check
* @see {@link match} for exhaustive pattern matching
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isSuccess = exitIsSuccess;
const DeferredProto = {
	["~effect/Deferred"]: {
		_A: identity$1,
		_E: identity$1
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
const DeferredImpl = function() {
	this.resumes = void 0;
	this.effect = void 0;
};
DeferredImpl.prototype = DeferredProto;
/**
* Creates an empty `Deferred` synchronously outside the `Effect` runtime.
*
* **When to use**
*
* Use to allocate a `Deferred` synchronously when direct allocation outside
* `Effect` is required.
*
* **Example** (Creating a Deferred unsafely)
*
* ```ts import.meta.vitest
* import { Deferred } from "effect"
*
* const deferred = Deferred.makeUnsafe<number>()
* Deferred.isDoneUnsafe(deferred) // => false
* ```
*
* @stability stable
* @category unsafe
* @since 4.0.0
*/
const makeUnsafe$4 = () => new DeferredImpl();
/**
* Completes the `Deferred` with the specified `Exit` value, which will be
* propagated to all fibers waiting on the value of the `Deferred`.
*
* **When to use**
*
* Use to complete a `Deferred` from an already computed `Exit`.
*
* **Details**
*
* The returned effect succeeds with `true` when this call completed the
* `Deferred`, or `false` if it was already completed.
*
* **Example** (Completing a Deferred with an Exit)
*
* ```ts import.meta.vitest
* import { Deferred, Effect, Exit } from "effect"
*
* const program = Effect.gen(function*() {
*   const deferred = yield* Deferred.make<number>()
*   yield* Deferred.done(deferred, Exit.succeed(42))
*   return yield* Effect.exit(Deferred.await(deferred))
* })
*
* await Effect.runPromise(program) // => Exit.succeed(42)
* ```
*
* @see {@link complete} for completing from an effect and memoizing its result
* @see {@link completeWith} for storing an effect directly
* @see {@link succeed} for completing with a success value
* @see {@link failCause} for completing with a failure cause
*
* @stability stable
* @category completion
* @since 2.0.0
*/
const done = /* @__PURE__ */ dual(2, (self, effect) => sync$1(() => doneUnsafe(self, effect)));
/**
* Attempts to complete the `Deferred` with the specified `Cause`.
*
* **When to use**
*
* Use to complete a `Deferred` with a full failure cause.
*
* **Details**
*
* Fibers waiting on the `Deferred` observe that cause only if this call
* completes it. The returned effect succeeds with `true` when this call
* completed the `Deferred`, or `false` if it was already completed.
*
* **Example** (Failing a Deferred with a Cause)
*
* ```ts import.meta.vitest
* import { Cause, Deferred, Effect, Exit } from "effect"
*
* const program = Effect.gen(function*() {
*   const deferred = yield* Deferred.make<number, string>()
*   const success = yield* Deferred.failCause(deferred, Cause.fail("Operation failed"))
*   const exit = yield* Effect.exit(Deferred.await(deferred))
*   return [success, exit]
* })
*
* await Effect.runPromise(program) // => [true, Exit.failCause(Cause.fail("Operation failed"))]
* ```
*
* @stability stable
* @category completion
* @since 2.0.0
*/
const failCause$1 = /*#__PURE__*/ dual(2, (self, cause) => done(self, exitFailCause(cause)));
/**
* Attempts to complete the `Deferred` with interruption by the specified
* `FiberId`.
*
* **When to use**
*
* Use to complete a `Deferred` as interrupted by a specific fiber id.
*
* **Details**
*
* Fibers waiting on the `Deferred` are interrupted with that fiber id only if
* this call completes it. The returned effect succeeds with `true` when this
* call completed the `Deferred`, or `false` if it was already completed.
*
* **Example** (Interrupting a Deferred with a fiber id)
*
* ```ts import.meta.vitest
* import { Deferred, Effect, Exit } from "effect"
*
* const program = Effect.gen(function*() {
*   const deferred = yield* Deferred.make<number>()
*   const success = yield* Deferred.interruptWith(deferred, 42)
*   const exit = yield* Effect.exit(Deferred.await(deferred))
*   return [success, exit]
* })
*
* await Effect.runPromise(program) // => [true, Exit.interrupt(42)]
* ```
*
* @stability stable
* @category completion
* @since 2.0.0
*/
const interruptWith = /*#__PURE__*/ dual(2, (self, fiberId) => failCause$1(self, causeInterrupt(fiberId)));
/**
* Attempts to complete the `Deferred` synchronously with the specified
* completion effect.
*
* **When to use**
*
* Use to complete a `Deferred` synchronously in low-level code that already has
* the completion effect.
*
* **Details**
*
* This mutates the `Deferred` directly and should be reserved for low-level
* code; prefer the effectful completion APIs when possible. Returns `true` if
* this call completed the `Deferred`, or `false` if it was already completed.
*
* **Example** (Completing a Deferred unsafely)
*
* ```ts import.meta.vitest
* import { Deferred, Effect } from "effect"
*
* const deferred = Deferred.makeUnsafe<number>()
* Deferred.doneUnsafe(deferred, Effect.succeed(42)) // => true
* ```
*
* @stability stable
* @category unsafe
* @since 4.0.0
*/
const doneUnsafe = (self, effect) => {
	if (self.effect) return false;
	self.effect = effect;
	if (self.resumes) {
		const resumes = self.resumes;
		self.resumes = void 0;
		for (let i = 0; i < resumes.length; i++) resumes[i](effect);
	}
	return true;
};
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Scope.js
/**
* Controls how long resources stay open.
*
* A scope is a lifetime boundary. Code can register cleanup effects on it, and
* closing the scope runs those cleanups with the `Exit` value that ended the
* work. Most application code uses higher-level APIs such as `Effect.scoped`
* and `Layer`, while this module is useful when code needs to create, provide,
* fork, close, or inspect scopes directly.
*
* @stability stable
* @since 2.0.0
*/
/**
* Service tag for the active resource lifetime.
*
* **When to use**
*
* Use to access the active lifetime when registering finalizers or sharing
* resources with the surrounding scope.
*
* **Example** (Accessing the scope service)
*
* ```ts import.meta.vitest
* import { Effect, Scope } from "effect"
*
* const cleanups: Array<string> = []
* const program = Effect.gen(function*() {
*   const scope = yield* Scope.Scope
*   yield* Scope.addFinalizer(scope, Effect.sync(() => cleanups.push("Cleanup")))
* })
*
* Effect.runSync(Effect.scoped(program))
* cleanups // => ["Cleanup"]
* ```
*
* @stability stable
* @category services
* @since 2.0.0
*/
const Scope = scopeTag;
/**
* Creates a new `Scope` synchronously without wrapping it in an `Effect`.
* This is useful when you need a scope immediately but should be used with caution
* as it doesn't provide the same safety guarantees as the `Effect`-wrapped version.
*
* **When to use**
*
* Use when a scope must be allocated synchronously and the caller will close it
* manually.
*
* **Example** (Creating a scope synchronously)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const scope = Scope.makeUnsafe("sequential")
* const cleanups: Array<string> = []
* const program = Effect.gen(function*() {
*   yield* Scope.addFinalizer(scope, Effect.sync(() => cleanups.push("Cleanup")))
*   yield* Scope.close(scope, Exit.void)
* })
*
* Effect.runSync(program)
* cleanups // => ["Cleanup"]
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const makeUnsafe$3 = scopeMakeUnsafe;
/**
* Provides a concrete `Scope` to an effect.
*
* **When to use**
*
* Use to run an effect that requires `Scope` with a scope managed by the
* caller.
*
* **Details**
*
* Providing the scope removes the `Scope` requirement from the effect context.
*
* **Example** (Providing a scope)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const events: Array<string> = []
* const program = Effect.gen(function*() {
*   const scope = yield* Scope.Scope
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("cleanup")))
*   events.push("working")
* })
*
* const withScope = Effect.gen(function*() {
*   const scope = yield* Scope.make()
*   yield* Scope.provide(scope)(program)
*   yield* Scope.close(scope, Exit.void)
* })
*
* Effect.runSync(withScope)
* events // => ["working", "cleanup"]
* ```
*
* @stability stable
* @category combinators
* @since 4.0.0
*/
const provide = provideScope;
/**
* Registers an exit-aware finalizer on a scope.
*
* **When to use**
*
* Use when cleanup needs to know whether the scope closed with success,
* failure, or interruption.
*
* **Details**
*
* If the scope is open, the finalizer runs when the scope closes and receives
* the scope's exit value. If the scope is already closed, the finalizer runs
* immediately with the stored exit value.
*
* **Example** (Adding an exit-aware finalizer)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const exits: Array<Exit.Exit<unknown, unknown>> = []
* const withResource = Effect.gen(function*() {
*   const scope = yield* Scope.make()
*   yield* Scope.addFinalizerExit(scope, (exit) => Effect.sync(() => exits.push(exit)))
*   yield* Scope.close(scope, Exit.void)
* })
*
* Effect.runSync(withResource)
* exits // => [Exit.void]
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const addFinalizerExit = scopeAddFinalizerExit;
/**
* Registers a finalizer effect on a scope.
*
* **Details**
*
* If the scope is open, the finalizer runs when the scope closes, regardless of
* whether the scope closes successfully or with an error. If the scope is
* already closed, the finalizer runs immediately.
*
* **Example** (Adding finalizers)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const events: Array<string> = []
* const program = Effect.gen(function*() {
*   const scope = yield* Scope.make()
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("cleanup 1")))
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("cleanup 2")))
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("cleanup 3")))
*   events.push("work")
*   yield* Scope.close(scope, Exit.void)
* })
*
* Effect.runSync(program)
* events // => ["work", "cleanup 3", "cleanup 2", "cleanup 1"]
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const addFinalizer$1 = scopeAddFinalizer;
/**
* Creates a closeable child scope synchronously and registers it with a parent scope.
*
* **When to use**
*
* Use when a child scope must be created synchronously and the caller controls
* both parent and child scope lifetimes.
*
* **Details**
*
* Closing the parent closes the child with the same exit value, and closing the
* child detaches it from the parent. The optional finalizer strategy configures
* the child scope and defaults to `"sequential"` when omitted.
*
* **Example** (Creating a child scope synchronously)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const cleanups: Array<string> = []
* const program = Effect.gen(function*() {
*   const parentScope = Scope.makeUnsafe("sequential")
*   const childScope = Scope.forkUnsafe(parentScope, "parallel")
*   yield* Scope.addFinalizer(parentScope, Effect.sync(() => cleanups.push("parent")))
*   yield* Scope.addFinalizer(childScope, Effect.sync(() => cleanups.push("child")))
*   yield* Scope.close(childScope, Exit.void)
*   yield* Scope.close(parentScope, Exit.void)
* })
*
* Effect.runSync(program)
* cleanups // => ["child", "parent"]
* ```
*
* @stability stable
* @category combinators
* @since 4.0.0
*/
const forkUnsafe = scopeForkUnsafe;
/**
* Closes a scope and runs its registered finalizers.
*
* **When to use**
*
* Use to close a scope manually with a specific exit value.
*
* **Details**
*
* Finalizers run in the scope's configured order and receive the supplied
* `Exit`.
* By default, finalizers run uninterruptibly, so interrupting the closing fiber
* waits for them to finish; a finalizer can explicitly restore interruptibility.
*
* **Gotchas**
*
* Closing a scope that is already closed returns immediately, even when the
* finalizers of an earlier close are still running.
*
* **Example** (Running scope finalizers)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const events: Array<string> = []
* const resourceManagement = Effect.gen(function*() {
*   const scope = yield* Scope.make("sequential")
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("database")))
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("file")))
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("memory")))
*   events.push("work")
*   yield* Scope.close(scope, Exit.succeed("Success!"))
* })
*
* Effect.runSync(resourceManagement)
* events // => ["work", "memory", "file", "database"]
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const close = scopeClose;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/dateTime.js
/** @internal */
const TypeId$15 = "~effect/DateTime";
/** @internal */
const TimeZoneTypeId = "~effect/DateTime/TimeZone";
const Proto$4 = {
	[TypeId$15]: TypeId$15,
	pipe() {
		return pipeArguments(this, arguments);
	},
	[NodeInspectSymbol]() {
		return this.toString();
	},
	toJSON() {
		return toDateUtc$1(this).toJSON();
	}
};
const ProtoUtc = {
	...Proto$4,
	_tag: "Utc",
	[symbol$1]() {
		return number$4(this.epochMilliseconds);
	},
	[symbol](that) {
		return isDateTime$1(that) && that._tag === "Utc" && this.epochMilliseconds === that.epochMilliseconds;
	},
	toString() {
		return `DateTime.Utc(${toDateUtc$1(this).toJSON()})`;
	}
};
const ProtoZoned = {
	...Proto$4,
	_tag: "Zoned",
	[symbol$1]() {
		return combine(number$4(this.epochMilliseconds))(hash(this.zone));
	},
	[symbol](that) {
		return isDateTime$1(that) && that._tag === "Zoned" && this.epochMilliseconds === that.epochMilliseconds && equals$1(this.zone, that.zone);
	},
	toString() {
		return `DateTime.Zoned(${formatIsoZoned(this)})`;
	}
};
const ProtoTimeZone = {
	[TimeZoneTypeId]: TimeZoneTypeId,
	[NodeInspectSymbol]() {
		return this.toString();
	}
};
({ ...ProtoTimeZone });
({ ...ProtoTimeZone });
const cacheSlot = {
	value: void 0,
	enumerable: false,
	writable: true
};
const sealedCaches = /*#__PURE__*/ new WeakMap();
const cacheAccessor = (key, slots) => ({
	get() {
		return Object.isExtensible(this) ? void 0 : sealedCaches.get(this)?.[key];
	},
	set(value) {
		if (Object.isExtensible(this)) {
			for (const slot of slots) Object.defineProperty(this, slot, cacheSlot);
			this[key] = value;
		} else {
			let caches = sealedCaches.get(this);
			if (caches === void 0) {
				caches = {};
				sealedCaches.set(this, caches);
			}
			caches[key] = value;
		}
	}
});
const zonedSlots = [
	"partsUtc",
	"adjustedEpochMilliseconds",
	"partsAdjusted"
];
Object.defineProperty(ProtoUtc, "partsUtc", cacheAccessor("partsUtc", ["partsUtc"]));
Object.defineProperties(ProtoZoned, {
	partsUtc: cacheAccessor("partsUtc", zonedSlots),
	adjustedEpochMilliseconds: cacheAccessor("adjustedEpochMilliseconds", zonedSlots),
	partsAdjusted: cacheAccessor("partsAdjusted", zonedSlots)
});
/** @internal */
const isDateTime$1 = (u) => hasProperty(u, TypeId$15);
/** @internal */
const toDateUtc$1 = (self) => new Date(self.epochMilliseconds);
/** @internal */
const toDate = (self) => {
	if (self._tag === "Utc") return new Date(self.epochMilliseconds);
	else if (self.zone._tag === "Offset") return new Date(self.epochMilliseconds + self.zone.offset);
	else if (self.adjustedEpochMilliseconds !== void 0) return new Date(self.adjustedEpochMilliseconds);
	let bc = false;
	const parts = self.zone.format.formatToParts(self.epochMilliseconds).filter((_) => {
		if (_.type === "era") {
			bc = _.value === "BC";
			return false;
		}
		return _.type !== "literal";
	});
	const year = Number(parts[2].value);
	const date = /* @__PURE__ */ new Date(0);
	date.setUTCFullYear(bc ? 1 - year : year, Number(parts[0].value) - 1, Number(parts[1].value));
	date.setUTCHours(Number(parts[3].value), Number(parts[4].value), Number(parts[5].value), parts[6]?.type === "fractionalSecond" ? Number(parts[6].value) : (self.epochMilliseconds % 1e3 + 1e3) % 1e3);
	self.adjustedEpochMilliseconds = date.getTime();
	return date;
};
/** @internal */
const zonedOffset = (self) => {
	return toDate(self).getTime() - toEpochMillis$1(self);
};
const offsetToString = (offset) => {
	const abs = Math.abs(offset);
	let hours = Math.floor(abs / 36e5);
	let minutes = Math.round(abs % 36e5 / 6e4);
	if (minutes === 60) {
		hours += 1;
		minutes = 0;
	}
	return `${offset < 0 ? "-" : "+"}${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
};
/** @internal */
const zonedOffsetIso = (self) => offsetToString(zonedOffset(self));
/** @internal */
const toEpochMillis$1 = (self) => self.epochMilliseconds;
/** @internal */
const formatIsoOffset = (self) => {
	const date = toDate(self);
	return self._tag === "Utc" ? date.toISOString() : `${date.toISOString().slice(0, -1)}${zonedOffsetIso(self)}`;
};
/** @internal */
const formatIsoZoned = (self) => self.zone._tag === "Offset" ? formatIsoOffset(self) : `${formatIsoOffset(self)}[${self.zone.id}]`;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Effect.js
/**
* Executes an effectful operation for each element in an `Iterable`.
*
* **When to use**
*
* Use to traverse an iterable with an effectful function while preserving
* element order in the collected results.
*
* **Details**
*
* The `forEach` function applies a provided operation to each element in the
* iterable, producing a new effect that returns an array of results.
*
* If any effect fails, the iteration stops immediately (short-circuiting), and
* the error is propagated.
*
* Concurrency:
*
* The `concurrency` option controls how many operations are performed
* concurrently. By default, the operations are performed sequentially.
*
* Discarding Results:
*
* If the `discard` option is set to `true`, the intermediate results are not
* collected, and the final result of the operation is `void`.
*
* **Example** (Mapping over an iterable with effects)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* const result = Effect.forEach(
*   [1, 2, 3, 4, 5],
*   (n, index) =>
*     Effect.sync(() => { output.push(`Currently at index ${index}`) }).pipe(Effect.as(n * 2))
* )
*
* void output.push(await Effect.runPromise(result))
* output // => ["Currently at index 0", "Currently at index 1", "Currently at index 2", "Currently at index 3", "Currently at index 4", [2, 4, 6, 8, 10]]
* ```
*
* **Example** (Running effects without collecting results)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* // Apply effects but discard the results
* const result = Effect.forEach(
*   [1, 2, 3, 4, 5],
*   (n, index) =>
*     Effect.sync(() => { output.push(`Currently at index ${index}`) }).pipe(Effect.as(n * 2)),
*   { discard: true }
* )
*
* void output.push(await Effect.runPromise(result))
* output // => ["Currently at index 0", "Currently at index 1", "Currently at index 2", "Currently at index 3", "Currently at index 4", undefined]
* ```
*
* @see {@link all} for combining multiple effects into one.
* @stability stable
* @category collecting
* @since 2.0.0
*/
const forEach = forEach$1;
/**
* Creates an `Effect` from an asynchronous computation that may throw or
* reject, mapping failures into the error channel.
*
* **When to use**
*
* Use when you need to perform asynchronous operations that might fail, such
* as fetching data from an API, and want thrown exceptions or rejected promises
* captured as Effect errors.
*
* **Details**
*
* The promise thunk is evaluated when the effect runs. If it returns a promise
* that resolves, the resolved value becomes the success value. If the thunk
* throws before returning a promise, or if the returned promise rejects, the
* thrown or rejected value is mapped into the error channel.
*
* Passing the thunk directly maps failures to {@link Cause.UnknownError}.
* Passing `{ try, catch }` uses `catch` to map failures to an error of type
* `E`.
*
* If the thunk declares a parameter, it receives an `AbortSignal` that is
* aborted if the effect is interrupted. The underlying asynchronous operation
* only stops if it observes that signal. Zero-argument thunks do not allocate
* an `AbortController`.
*
* **Gotchas**
*
* If `catch` throws while mapping the error, that thrown value is treated as a
* defect. Return the error value you want in the error channel instead of
* throwing it.
*
* The parameter check reads the thunk's `length` property, which does not
* count a rest parameter or any parameter from the first default value
* onward. `(...args) => ...` and `(signal = fallback) => ...` therefore
* receive `undefined` instead of a signal, as do wrappers that forward
* arguments through rest parameters. Declare the signal as an ordinary
* parameter: `(signal) => ...`.
*
* **Example** (Wrapping a fetch request that may fail)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const getTodo = (id: number) =>
*   Effect.tryPromise(() => Promise.resolve({ id, completed: false }))
*
* //      ┌─── Effect<{ id: number; completed: boolean }, UnknownError, never>
* //      ▼
* const program = getTodo(1)
* await Effect.runPromise(program) // => { id: 1, completed: false }
* ```
*
* **Example** (Mapping Promise rejections to a tagged error)
*
* ```ts import.meta.vitest
* import { Data, Effect } from "effect"
*
* class TodoFetchError extends Data.TaggedError("TodoFetchError")<{ readonly cause: unknown }> {}
*
* const getTodo = (id: number) =>
*   Effect.tryPromise({
*     try: () => Promise.reject(`Todo ${id} is unavailable`),
*     // remap the error
*     catch: (cause) => new TodoFetchError({ cause })
*   })
*
* //      ┌─── Effect<never, TodoFetchError, never>
* //      ▼
* const program = Effect.flip(getTodo(1))
* const error = await Effect.runPromise(program)
* error._tag // => "TodoFetchError"
* ```
*
* @see {@link promise} if the effectful computation is asynchronous and does not throw errors.
* @stability stable
* @category constructors
* @since 2.0.0
*/
const tryPromise = tryPromise$1;
/**
* Creates an `Effect` that always succeeds with a given value.
*
* **When to use**
*
* Use when an effect should complete successfully with a specific value without any errors
* or external dependencies.
*
* **Example** (Creating a successful effect)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* // Creating an effect that represents a successful scenario
* //
* //      ┌─── Effect<number, never, never>
* //      ▼
* const success = Effect.succeed(42)
* Effect.runSync(success) // => 42
* ```
*
* @see {@link fail} to create an effect that represents a failure.
* @stability stable
* @category constructors
* @since 2.0.0
*/
const succeed$1 = succeed$3;
/**
* Creates an `Effect` lazily, delaying construction until it is needed.
*
* **When to use**
*
* Use when you need to defer the evaluation of an effect until it is required.
*
* **Details**
*
* `suspend` takes a thunk that represents an effect and delays creating it
* until the suspended effect is evaluated. This is useful for optimizing
* expensive computations, managing circular dependencies such as recursive
* functions, and helping TypeScript unify return types when branches construct
* different effects. Any side effects or scoped captures inside the thunk are
* re-executed on each invocation.
*
* **Example** (Lazily evaluating side effects)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* let i = 0
*
* const bad = Effect.succeed(i++)
*
* const good = Effect.suspend(() => Effect.succeed(i++))
*
* Effect.runSync(bad) // => 0
* Effect.runSync(bad) // => 0
*
* Effect.runSync(good) // => 1
* Effect.runSync(good) // => 2
* ```
*
* **Example** (Suspending recursive Fibonacci evaluation)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const blowsUp = (n: number): Effect.Effect<number> =>
*   n < 2
*     ? Effect.succeed(1)
*     : Effect.zipWith(blowsUp(n - 1), blowsUp(n - 2), (a, b) => a + b)
*
* // console.log(Effect.runSync(blowsUp(32)))
* // crash: JavaScript heap out of memory
*
* const allGood = (n: number): Effect.Effect<number> =>
*   n < 2
*     ? Effect.succeed(1)
*     : Effect.zipWith(
*         Effect.suspend(() => allGood(n - 1)),
*         Effect.suspend(() => allGood(n - 2)),
*         (a, b) => a + b
*       )
*
* Effect.runSync(allGood(16)) // => 1597
* ```
*
* **Example** (Helping TypeScript infer recursive effect types)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* //   Without suspend, TypeScript may struggle with type inference.
* //   Inferred type:
* //     (a: number, b: number) =>
* //       Effect<never, Error, never> | Effect<number, never, never>
* const withoutSuspend = (a: number, b: number) =>
*   b === 0
*     ? Effect.fail(new Error("Cannot divide by zero"))
*     : Effect.succeed(a / b)
*
* //   Using suspend to unify return types.
* //   Inferred type:
* //     (a: number, b: number) => Effect<number, Error, never>
* const withSuspend = (a: number, b: number) =>
*   Effect.suspend(() =>
*     b === 0
*       ? Effect.fail(new Error("Cannot divide by zero"))
*       : Effect.succeed(a / b)
*   )
*
* Effect.runSync(withSuspend(6, 2)) // => 3
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const suspend = suspend$1;
/**
* Creates an `Effect` that represents a synchronous side-effectful computation.
*
* **When to use**
*
* Use when you need to wrap a synchronous side-effectful operation that is not
* expected to throw.
*
* **Details**
*
* The provided function is evaluated lazily when the effect runs.
*
* **Gotchas**
*
* The function must not throw. If it throws, the thrown value is treated as a
* defect, not as a typed failure. Use `try` when throwing is expected.
*
* **Example** (Capturing synchronous logging in an Effect)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* const log = (message: string) =>
*   Effect.sync(() => {
*     void output.push(message) // side effect
*   })
*
* //      ┌─── Effect<void, never, never>
* //      ▼
* const program = log("Hello, World!")
* Effect.runSync(program)
* output // => ["Hello, World!"]
* ```
*
* @see {@link try_ | try} for a version that can handle failures.
* @stability stable
* @category constructors
* @since 2.0.0
*/
const sync = sync$1;
const void_ = void_$2;
/**
* Creates an `Effect` from a callback-based asynchronous API.
*
* **When to use**
*
* Use when you need to integrate APIs that complete through callbacks instead
* of returning a `Promise`.
*
* **Details**
*
* The registration function receives a `resume` callback and, if it declares
* a second parameter, an `AbortSignal` that is aborted if the fiber is
* interrupted. Call `resume` at most once with the effect that should complete
* the fiber; later calls are ignored. Return an optional cleanup effect from
* the registration function to run if the fiber is interrupted.
*
* **Gotchas**
*
* The parameter check reads the registration function's `length` property,
* which must be at least `2` and does not count a rest parameter or any
* parameter from the first default value onward. `(...args) => ...` and
* `(resume, signal = fallback) => ...` therefore receive `undefined` instead
* of a signal, as do wrappers that forward arguments through rest parameters.
* Declare both as ordinary parameters: `(resume, signal) => ...`.
*
* **Example** (Integrating callback APIs)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* const fromCallback = (message: string) =>
*   Effect.callback<void>((resume) => {
*     queueMicrotask(() => {
*       void output.push(message)
*       resume(Effect.void)
*     })
*   })
*
* await Effect.runPromise(fromCallback("callback completed"))
* output // => ["callback completed"]
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const callback = callback$1;
/**
* Creates an `Effect` that represents a recoverable error.
*
* **When to use**
*
* Use to explicitly signal a recoverable error in an `Effect`.
*
* **Details**
*
* The error keeps propagating unless it is handled. You can handle tagged
* errors with functions like {@link catchTag} or {@link catchTags}.
*
* **Example** (Creating a failed effect)
*
* ```ts import.meta.vitest
* import { Data, Effect } from "effect"
*
* class OperationFailedError extends Data.TaggedError("OperationFailedError")<{}> {}
*
* //      ┌─── Effect<never, OperationFailedError, never>
* //      ▼
* const failure = Effect.fail(
*   new OperationFailedError()
* )
* Effect.runSync(Effect.flip(failure))._tag // => "OperationFailedError"
* ```
*
* @see {@link succeed} to create an effect that represents a successful value.
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fail$1 = fail$3;
/**
* Creates an `Effect` that represents a failure with a specific `Cause`.
*
* **When to use**
*
* Use when you already have a full `Cause` and need to preserve defects,
* interruptions, annotations, or combined failures in the effect's failure
* channel.
*
* **Details**
*
* This function allows you to create effects that fail with complex error
* structures, including multiple errors, defects, interruptions, and more.
*
* **Example** (Failing with a full Cause)
*
* ```ts import.meta.vitest
* import { Cause, Effect } from "effect"
*
* const program = Effect.failCause(
*   Cause.fail("Network error")
* )
*
* Effect.runSync(Effect.flip(program)) // => "Network error"
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const failCause = failCause$3;
/**
* Creates an `Effect` that represents a failure with a `Cause` computed lazily.
*
* **When to use**
*
* Use to defer computing a full `Cause` until the effect is run.
*
* **Details**
*
* The cause-producing function is evaluated each time the effect is executed.
*
* **Example** (Lazily creating a Cause)
*
* ```ts import.meta.vitest
* import { Cause, Effect } from "effect"
*
* const program = Effect.failCauseSync(() =>
*   Cause.fail("Error computed at runtime")
* )
*
* Effect.runSync(Effect.flip(program)) // => "Error computed at runtime"
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const failCauseSync = failCauseSync$1;
/**
* Creates an effect that terminates a fiber with a specified error.
*
* **When to use**
*
* Use when you need an `Effect` to report an unrecoverable defect instead of a
* typed error.
*
* **Details**
*
* The `die` function is used to signal a defect, which represents a critical
* and unexpected error in the code. When invoked, it produces an effect that
* does not handle the error and instead terminates the fiber.
*
* The error channel of the resulting effect is of type `never`, indicating that
* it cannot recover from this failure.
*
* **Example** (Failing on division by zero)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
*
* const defect = new Error("Cannot divide by zero")
* const divide = (a: number, b: number) =>
*   b === 0
*     ? Effect.die(defect)
*     : Effect.succeed(a / b)
*
* //      ┌─── Effect<number, never, never>
* //      ▼
* const program = divide(1, 0)
*
* Effect.runSyncExit(program) // => Exit.die(defect)
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const die = die$2;
/**
* Provides access to the current fiber within an effect computation.
*
* **Example** (Reading the current fiber)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const program = Effect.withFiber((fiber) => Effect.succeed(typeof fiber.id))
*
* Effect.runSync(program) // => "number"
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const withFiber = withFiber$1;
/**
* Chains effects to produce new `Effect` instances, useful for combining
* operations that depend on previous results.
*
* **When to use**
*
* Use when you need to chain multiple effects, ensuring that each
* step produces a new `Effect` while flattening any nested effects that may
* occur.
*
* **Details**
*
* `flatMap` lets you sequence effects so that the result of one effect can be
* used in the next step. It is similar to `flatMap` used with arrays but works
* specifically with `Effect` instances, allowing you to avoid deeply nested
* effect structures.
*
* Since effects are immutable, `flatMap` always returns a new effect instead of
* changing the original one.
*
* **Example** (Choosing flatMap syntax variants)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
* const output: Array<unknown> = []
*
* const myEffect = Effect.succeed(1)
* const transformation = (n: number) => Effect.succeed(n + 1)
*
* const flatMappedWithPipe = pipe(myEffect, Effect.flatMap(transformation))
* const flatMappedWithDataFirst = Effect.flatMap(myEffect, transformation)
* const flatMappedWithMethod = myEffect.pipe(Effect.flatMap(transformation))
*
* void output.push(Effect.runSync(Effect.all([
*   flatMappedWithPipe,
*   flatMappedWithDataFirst,
*   flatMappedWithMethod
* ])))
* output // => [[2, 2, 2]]
* ```
*
* **Example** (Sequencing dependent effects)
*
* ```ts import.meta.vitest
* import { Data, Effect, pipe } from "effect"
*
* class DiscountRateError extends Data.TaggedError("DiscountRateError")<{}> {}
*
* // Function to apply a discount safely to a transaction amount
* const applyDiscount = (
*   total: number,
*   discountRate: number
* ): Effect.Effect<number, DiscountRateError> =>
*   discountRate === 0
*     ? Effect.fail(new DiscountRateError())
*     : Effect.succeed(total - (total * discountRate) / 100)
*
* // Simulated asynchronous task to fetch a transaction amount from database
* const fetchTransactionAmount = Effect.promise(() => Promise.resolve(100))
*
* // Chaining the fetch and discount application using `flatMap`
* const finalAmount = pipe(
*   fetchTransactionAmount,
*   Effect.flatMap((amount) => applyDiscount(amount, 5))
* )
*
* await Effect.runPromise(finalAmount) // => 95
* ```
*
* @see {@link tap} for a version that ignores the result of the effect.
* @stability stable
* @category sequencing
* @since 2.0.0
*/
const flatMap = flatMap$1;
/**
* Runs this effect and then runs another effect, optionally using the first
* effect's success value to choose the next effect.
*
* **When to use**
*
* Use when you need one effect to run after another and the second effect may
* depend on the first effect's success value.
*
* **Details**
*
* When the second argument is an `Effect`, the first success value is discarded
* and the returned effect produces the second effect's value. When the second
* argument is a function, it receives the first success value and must return
* the next `Effect`.
*
* Failures or requirements from either effect are preserved in the returned
* effect.
*
* **Example** (Choosing andThen syntax variants)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
* const output: Array<unknown> = []
*
* const myEffect = Effect.succeed(1)
* const anotherEffect = Effect.succeed("done")
*
* const transformedWithPipe = pipe(myEffect, Effect.andThen(anotherEffect))
* const transformedWithDataFirst = Effect.andThen(myEffect, anotherEffect)
* const transformedWithMethod = myEffect.pipe(Effect.andThen(anotherEffect))
*
* void output.push(Effect.runSync(Effect.all([
*   transformedWithPipe,
*   transformedWithDataFirst,
*   transformedWithMethod
* ])))
* output // => [['done', 'done', 'done']]
* ```
*
* **Example** (Sequencing a discount calculation after fetching a total)
*
* ```ts import.meta.vitest
* import { Data, Effect, pipe } from "effect"
*
* class DiscountRateError extends Data.TaggedError("DiscountRateError")<{}> {}
*
* // Function to apply a discount safely to a transaction amount
* const applyDiscount = (
*   total: number,
*   discountRate: number
* ): Effect.Effect<number, DiscountRateError> =>
*   discountRate === 0
*     ? Effect.fail(new DiscountRateError())
*     : Effect.succeed(total - (total * discountRate) / 100)
*
* // Simulated asynchronous task to fetch a transaction amount from database
* const fetchTransactionAmount = Effect.promise(() => Promise.resolve(100))
*
* // Using Effect.map and Effect.flatMap
* const result1 = pipe(
*   fetchTransactionAmount,
*   Effect.map((amount) => amount * 2),
*   Effect.flatMap((amount) => applyDiscount(amount, 5))
* )
*
* await Effect.runPromise(result1) // => 190
*
* // Using Effect.andThen
* const result2 = pipe(
*   fetchTransactionAmount,
*   Effect.andThen((amount) => Effect.succeed(amount * 2)),
*   Effect.andThen((amount) => applyDiscount(amount, 5))
* )
*
* await Effect.runPromise(result2) // => 190
* ```
*
* @stability stable
* @category sequencing
* @since 2.0.0
*/
const andThen = andThen$1;
/**
* Runs a side effect with the result of an effect without changing the original
* value.
*
* **When to use**
*
* Use when you need to run an effectful observation, such as logging or
* tracking, while passing the original success value to the next step.
*
* **Details**
*
* `tap` works similarly to `flatMap`, but it ignores the result of the function
* passed to it. The value from the previous effect remains available for the
* next part of the chain. Note that if the side effect fails, the entire chain
* will fail too.
*
* **Example** (Logging a step in a pipeline)
*
* ```ts import.meta.vitest
* import { Data, Effect, pipe } from "effect"
* const output: Array<unknown> = []
*
* class DiscountRateError extends Data.TaggedError("DiscountRateError")<{}> {}
*
* // Function to apply a discount safely to a transaction amount
* const applyDiscount = (
*   total: number,
*   discountRate: number
* ): Effect.Effect<number, DiscountRateError> =>
*   discountRate === 0
*     ? Effect.fail(new DiscountRateError())
*     : Effect.succeed(total - (total * discountRate) / 100)
*
* // Simulated asynchronous task to fetch a transaction amount from database
* const fetchTransactionAmount = Effect.promise(() => Promise.resolve(100))
*
* const finalAmount = pipe(
*   fetchTransactionAmount,
*   // Log the fetched transaction amount
*   Effect.tap((amount) => Effect.sync(() => { output.push(`Apply a discount to: ${amount}`) })),
*   // `amount` is still available!
*   Effect.flatMap((amount) => applyDiscount(amount, 5))
* )
*
* void output.push(await Effect.runPromise(finalAmount))
* output // => ["Apply a discount to: 100", 95]
* ```
*
* @stability stable
* @category sequencing
* @since 2.0.0
*/
const tap = tap$1;
/**
* Transforms an effect to encapsulate both failure and success using the `Exit`
* data type.
*
* **When to use**
*
* Use when you need to inspect the full outcome, including typed failures, defects,
* and interruptions.
*
* **Details**
*
* `exit` wraps an effect's success or failure inside an `Exit` type, allowing
* you to handle both cases explicitly.
*
* The resulting effect cannot fail because the failure is encapsulated within
* the `Exit.Failure` type. The error type is set to `never`, indicating that
* the effect is structured to never fail directly.
*
* **Example** (Capturing completion as Exit)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
*
* const success = Effect.succeed(42)
* const failure = Effect.fail("Something went wrong")
*
* const program1 = Effect.exit(success)
* const program2 = Effect.exit(failure)
*
* Effect.runSync(program1) // => Exit.succeed(42)
*
* Effect.runSync(program2) // => Exit.fail("Something went wrong")
* ```
*
* @see {@link option} for a version that uses `Option` instead.
* @see {@link result} for a version that uses `Result` instead.
*
* @stability stable
* @category error handling
* @since 2.0.0
*/
const exit = exit$1;
/**
* Transforms the value inside an effect by applying a function to it.
*
* **When to use**
*
* Use to transform an effect's success value with a function that returns a
* plain value, producing a new effect without changing the original effect's
* typed error or context requirements.
*
* **Details**
*
* `map` takes a function and applies it to the value contained within an
* effect, creating a new effect with the transformed value.
*
* It's important to note that effects are immutable, meaning that the original
* effect is not modified. Instead, a new effect is returned with the updated
* value.
*
* **Example** (Choosing map syntax variants)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
* const output: Array<unknown> = []
*
* const myEffect = Effect.succeed(1)
* const transformation = (n: number) => n + 1
*
* const mappedWithPipe = pipe(myEffect, Effect.map(transformation))
* const mappedWithDataFirst = Effect.map(myEffect, transformation)
* const mappedWithMethod = myEffect.pipe(Effect.map(transformation))
*
* void output.push(Effect.runSync(Effect.all([
*   mappedWithPipe,
*   mappedWithDataFirst,
*   mappedWithMethod
* ])))
* output // => [[2, 2, 2]]
* ```
*
* **Example** (Adding a service charge)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
*
* const addServiceCharge = (amount: number) => amount + 1
*
* const fetchTransactionAmount = Effect.promise(() => Promise.resolve(100))
*
* const finalAmount = pipe(
*   fetchTransactionAmount,
*   Effect.map(addServiceCharge)
* )
*
* await Effect.runPromise(finalAmount) // => 101
* ```
*
* @see {@link mapError} for a version that operates on the error channel.
* @see {@link mapBoth} for a version that operates on both channels.
* @see {@link flatMap} or {@link andThen} for a version that can return a new effect.
* @stability stable
* @category mapping
* @since 2.0.0
*/
const map = map$2;
/**
* Replaces the value inside an effect with a constant value.
*
* **When to use**
*
* Use to replace a successful value with a constant while preserving failures
* and requirements.
*
* **Details**
*
* `as` allows you to ignore the original value inside an effect and
* replace it with a new constant value.
*
* **Example** (Replacing a success value)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
*
* // Replaces the value 5 with the constant "new value"
* const program = pipe(Effect.succeed(5), Effect.as("new value"))
*
* Effect.runSync(program) // => "new value"
* ```
*
* @see {@link map} for deriving the replacement value from the success value
* @see {@link asVoid} for replacing the success value with `void`
*
* @stability stable
* @category mapping
* @since 2.0.0
*/
const as = as$1;
/**
* Maps the success value of an `Effect` to `void`, preserving failures.
*
* **Example** (Discarding success values)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const program = Effect.asVoid(Effect.succeed(42))
*
* Effect.runSync(program) // => undefined
* ```
*
* @stability stable
* @category mapping
* @since 2.0.0
*/
const asVoid = asVoid$1;
const catch_ = catch_$1;
/**
* Handles both recoverable and unrecoverable errors by providing a recovery
* effect.
*
* **When to use**
*
* Use when you need to recover from an `Effect` by inspecting the full `Cause`,
* including recoverable failures, defects, and interruptions, instead of only
* the typed error value.
*
* **Details**
*
* When to Recover from Defects:
*
* Defects are unexpected errors that typically shouldn't be recovered from, as
* they often indicate serious issues. However, in some cases, such as
* dynamically loaded plugins, controlled recovery might be needed.
*
* **Example** (Recovering from full failure causes)
*
* ```ts import.meta.vitest
* import { Cause, Effect } from "effect"
* const output: Array<unknown> = []
*
* // An effect that might fail in different ways
* const program = Effect.die("Something went wrong")
*
* // Recover from any cause (including defects)
* const recovered = Effect.catchCause(program, (cause) => {
*   if (Cause.hasDies(cause)) {
*     return Effect.sync(() => { output.push("Caught defect") }).pipe(
*       Effect.as("Recovered from defect")
*     )
*   }
*   return Effect.succeed("Unknown error")
* })
*
* void output.push(Effect.runSync(recovered))
* output // => ["Caught defect", "Recovered from defect"]
* ```
*
* @stability stable
* @category error handling
* @since 4.0.0
*/
const catchCause = catchCause$1;
/**
* Runs an effect conditionally based on the result of an effectful boolean
* condition.
*
* **When to use**
*
* Use when you need an effectful check to decide whether another effect should
* run while representing the skipped case explicitly.
*
* **Details**
*
* The condition effect is evaluated first. If it succeeds with `true`, the
* source effect is run and its success value is wrapped in `Option.some`. If it
* succeeds with `false`, the source effect is skipped and the result is
* `Option.none`. If the condition effect fails, that failure is preserved.
*
* **Example** (Conditionally running an effect)
*
* ```ts import.meta.vitest
* import { Effect, Option } from "effect"
* const output: Array<unknown> = []
*
* const shouldLog = true
*
* const program = Effect.when(
*   Effect.sync(() => { output.push("Condition is true!") }),
*   Effect.succeed(shouldLog)
* )
*
* void output.push(Effect.runSync(program))
* output // => ["Condition is true!", Option.some(undefined)]
* ```
*
* @stability stable
* @category filtering
* @since 2.0.0
*/
const when = when$1;
/**
* Transforms the current context using the provided function.
*
* **When to use**
*
* Use to derive an effect from the complete `Context`.
*
* **Details**
*
* This function allows you to access the complete context and perform
* computations based on all available services. This is useful when you need
* to conditionally execute logic based on what services are available.
*
* **Example** (Deriving values from the context)
*
* ```ts import.meta.vitest
* import { Context, Effect, Option } from "effect"
* const output: Array<unknown> = []
*
* const Logger = Context.Service<{
*   log: (msg: string) => void
* }>("Logger")
* const Cache = Context.Service<{
*   get: (key: string) => string | null
* }>("Cache")
*
* const program = Effect.contextWith((services: Context.Context<Context.Service.Identifier<typeof Cache>>) => {
*   const cacheOption = Context.getOption(services, Cache)
*   const hasCache = Option.isSome(cacheOption)
*
*   if (hasCache) {
*     return Effect.gen(function*() {
*       const cache = yield* Effect.service(Cache)
*       yield* Effect.sync(() => { output.push("Using cached data") })
*       return cache.get("user:123") || "default"
*     })
*   } else {
*     return Effect.gen(function*() {
*       yield* Effect.sync(() => { output.push("No cache available, using fallback") })
*       return "fallback data"
*     })
*   }
* })
*
* const withCache = Effect.provideService(program, Cache, {
*   get: () => "cached_value"
* })
* void output.push(Effect.runSync(withCache))
* output // => ["Using cached data", "cached_value"]
* ```
*
* @see {@link context} for reading the complete context as a value
* @see {@link service} for reading one service from the context
*
* @stability stable
* @category accessors
* @since 2.0.0
*/
const contextWith = contextWith$1;
/**
* Adds a finalizer to the current scope.
*
* **When to use**
*
* Use to register low-level cleanup in the current scope.
*
* **Details**
*
* The finalizer runs when the surrounding scope is closed and receives the
* `Exit` value used to close the scope.
*
* **Example** (Registering scope finalizers)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
* const output: Array<unknown> = []
*
* const program = Effect.scoped(
*   Effect.gen(function*() {
*     // Add a finalizer that runs when the scope closes
*     yield* Effect.addFinalizer((exit) =>
*       Effect.sync(() => { output.push(
*         Exit.isSuccess(exit)
*           ? "Cleanup: Operation completed successfully"
*           : "Cleanup: Operation failed, cleaning up resources"
*       ) })
*     )
*
*     yield* Effect.sync(() => { output.push("Performing main operation...") })
*
*     // This could succeed or fail
*     return "operation result"
*   })
* )
*
* void output.push(Effect.runSync(program))
* output // => ["Performing main operation...", "Cleanup: Operation completed successfully", "operation result"]
* ```
*
* @see {@link acquireRelease} for resource acquisition with a release finalizer
* @see {@link ensuring} for attaching a finalizer to one effect
*
* @stability stable
* @category resource management
* @since 2.0.0
*/
const addFinalizer = addFinalizer$2;
/**
* Returns an effect that is immediately interrupted.
*
* **Example** (Creating an interrupted effect)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const program = Effect.gen(function*() {
*   return yield* Effect.interrupt
*   yield* Effect.succeed("This won't execute and is unreachable")
* })
*
* Effect.runSyncExit(program)._tag // => "Failure"
* ```
*
* @stability stable
* @category interruption
* @since 2.0.0
*/
const interrupt = interrupt$2;
/**
* Runs the specified finalizer effect if this effect is interrupted.
*
* **Example** (Running cleanup on interruption)
*
* ```ts import.meta.vitest
* import { Effect, Fiber } from "effect"
* const output: Array<unknown> = []
*
* const task = Effect.forever(Effect.succeed("working..."))
*
* const program = Effect.onInterrupt(
*   task,
*   () => Effect.sync(() => { output.push("Task was interrupted, cleaning up...") })
* )
*
* const fiber = Effect.runFork(program)
* await Effect.runPromise(Fiber.interrupt(fiber))
* output // => ["Task was interrupted, cleaning up..."]
* ```
*
* @stability stable
* @category interruption
* @since 2.0.0
*/
const onInterrupt = onInterrupt$1;
/**
* Returns a new effect that disables interruption for the given effect.
*
* **Example** (Preventing interruption)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* const criticalTask = Effect.gen(function*() {
*   yield* Effect.sync(() => { output.push("Starting critical section...") })
*   yield* Effect.sync(() => { output.push("Critical section completed") })
* })
*
* const program = Effect.uninterruptible(criticalTask)
*
* Effect.runSync(program)
* output // => ["Starting critical section...", "Critical section completed"]
* ```
*
* @stability stable
* @category interruption
* @since 2.0.0
*/
const uninterruptible = uninterruptible$1;
/**
* Executes an effect and returns the result as a `Promise`.
*
* **When to use**
*
* Use when you need to execute an effect and work with the
* result using `Promise` syntax, typically for compatibility with other
* promise-based code.
*
* If the effect succeeds, the promise will resolve with the result. If the
* effect fails, the promise will reject with an error.
*
* **Example** (Running a successful effect as a Promise)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* await Effect.runPromise(Effect.succeed(1)) // => 1
* ```
*
* **Example** (Running effects as promises)
*
* ```ts import.meta.vitest
* //Example: Handling a Failing Effect as a Rejected Promise
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* await Effect.runPromise(Effect.fail("my error")).catch(() => {
*   void output.push("rejected")
* })
* output // => ["rejected"]
* ```
*
* @see {@link runPromiseExit} for a version that returns an `Exit` type instead of rejecting.
* @stability stable
* @category running
* @since 2.0.0
*/
const runPromise = runPromise$1;
/**
* Runs an effect synchronously and captures the outcome safely as an `Exit` type, which
* represents the outcome (success or failure) of the effect.
*
* **When to use**
*
* Use to find out whether an effect succeeded or failed,
* including any defects, without dealing with asynchronous operations.
*
* **Details**
*
* The `Exit` type represents the result of the effect. Successful effects are
* wrapped in `Success`, and failed effects are wrapped in `Failure` with a
* `Cause`.
*
* If the effect contains asynchronous operations, `runSyncExit` will
* return an `Failure` with a `Die` cause, indicating that the effect cannot be
* resolved synchronously.
*
* **Example** (Observing synchronous results as Exit)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
*
* Effect.runSyncExit(Effect.succeed(1)) // => Exit.succeed(1)
*
* Effect.runSyncExit(Effect.fail("my error")) // => Exit.fail("my error")
* ```
*
* **Example** (Capturing async work as a Die cause)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Exit } from "effect"
*
* const exit = Effect.runSyncExit(Effect.promise(() => Promise.resolve(1)))
* const isAsyncDie = Exit.isFailure(exit) && Exit.hasDies(exit) && exit.cause.reasons.some(
*   (reason) => Cause.isDieReason(reason) && Cause.isAsyncFiberError(reason.defect)
* )
*
* isAsyncDie // => true
* ```
*
* @see {@link runSync} for a version that throws on failure.
*
* @stability stable
* @category running
* @since 2.0.0
*/
const runSyncExit = runSyncExit$1;
/**
* Applies `map` eagerly when an effect is already resolved.
*
* **When to use**
*
* Use when an already-resolved effect should apply a success transformation
* immediately while pending effects still use regular mapping.
*
* **Details**
*
* Success effects apply the mapping function immediately. Failure effects pass
* through unchanged, and pending effects fall back to regular `map` behavior.
*
* **Example** (Mapping already completed effects)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* // For resolved effects, the mapping is applied immediately
* const resolved = Effect.succeed(5)
* const mapped = Effect.mapEager(resolved, (n) => n * 2) // Applied eagerly
*
* // For pending effects, behaves like regular map
* const pending = Effect.delay(Effect.succeed(5), 0)
* const mappedPending = Effect.mapEager(pending, (n) => n * 2) // Uses regular map
*
* await Effect.runPromise(Effect.all([mapped, mappedPending])) // => [10, 10]
* ```
*
* @stability stable
* @category mapping
* @since 4.0.0
*/
const mapEager = mapEager$1;
/**
* Applies `flatMap` eagerly when an effect is already resolved.
*
* **When to use**
*
* Use when an already-resolved successful effect should bind immediately to the
* next effect while pending effects still use regular flat mapping.
*
* **Details**
*
* Success effects apply the flatMap function immediately. Failure effects pass
* through unchanged, and pending effects fall back to regular `flatMap`
* behavior.
*
* **Example** (Flat mapping eagerly when possible)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* // For resolved effects, the flatMap is applied immediately
* const resolved = Effect.succeed(5)
* const flatMapped = Effect.flatMapEager(resolved, (n) => Effect.succeed(n * 2)) // Applied eagerly
*
* // For pending effects, behaves like regular flatMap
* const pending = Effect.delay(Effect.succeed(5), 0)
* const flatMappedPending = Effect.flatMapEager(
*   pending,
*   (n) => Effect.succeed(n * 2)
* ) // Uses regular flatMap
*
* await Effect.runPromise(Effect.all([flatMapped, flatMappedPending])) // => [10, 10]
* ```
*
* @stability stable
* @category sequencing
* @since 4.0.0
*/
const flatMapEager = flatMapEager$1;
/**
* Creates untraced function effects with eager evaluation optimization.
*
* **Details**
*
* Executes generator functions eagerly when all yielded effects are synchronous,
* stopping at the first async effect and deferring to normal execution.
* Additional arguments after the generator body act like `pipe` transforms, and
* `{ self }` before the body binds `this`, as with {@link fnUntraced}.
*
* **Example** (Defining eager untraced effect functions)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const computation = Effect.fnUntracedEager(function*() {
*   yield* Effect.succeed(1)
*   yield* Effect.succeed(2)
*   return "computed eagerly"
* })
*
* const effect = computation() // Executed immediately if all effects are sync
* Effect.runSync(effect) // => "computed eagerly"
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fnUntracedEager = fnUntracedEager$1;
const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const codes = /*#__PURE__*/ (/* @__PURE__ */ new Uint8Array(123)).fill(255);
for (let i = 0; i < 64; i++) codes[/*#__PURE__*/ alphabet.charCodeAt(i)] = i;
codes[/*#__PURE__*/ "=".charCodeAt(0)] = 0;
({ ...BaseProto });
({ ...BaseProto });
({ ...PipeInspectableProto });
BaseProto.toString, BaseProto[NodeInspectSymbol];
({ ...PipeInspectableProto });
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/schema/annotations.js
/** @internal */
const STRUCTURAL_ANNOTATION_KEY = "~structural";
/** @internal */
const SENTINELS_ANNOTATION_KEY = "~sentinels";
/** @internal */
const CONSTRUCTOR_ANNOTATION_KEY = "~constructor";
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/schema/parser.js
/** @internal */
const missing = /*#__PURE__*/ Symbol();
/** @internal */
const succeed = succeed$2;
/** @internal */
const missingExit = /*#__PURE__*/ succeed(missing);
/** @internal */
const sameExit = /*#__PURE__*/ succeed(missing);
/** @internal */
const toOption = (value) => value === missing ? none() : some(value);
/** @internal */
const fromOptionExit = (option) => option._tag === "None" ? missingExit : succeed(option.value);
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/SchemaIssue.js
/**
* Describes problems found while decoding, encoding, or checking data with
* schemas.
*
* An `Issue` records what failed and, for nested data, where the failure
* happened. The Schema system uses these values for missing keys, unexpected
* keys, invalid types, invalid values, failed filters, failed transformations,
* and alternatives that did not match. This module also formats issues.
*
* @stability stable
* @since 4.0.0
*/
const TypeId$10 = "~effect/SchemaIssue/Issue";
/**
* Returns `true` if the given value is an {@link Issue}.
*
* **When to use**
*
* Use when you need to narrow an `unknown` value to `Issue` in error-handling
* code, such as distinguishing an `Issue` from other error types in a catch-all
* handler.
*
* **Details**
*
* - Checks for the internal `TypeId` brand on the value.
*
* **Example** (Type-guarding an unknown error)
*
* ```ts import.meta.vitest
* import { SchemaIssue } from "effect"
*
* const issue = new SchemaIssue.MissingKey(undefined)
* SchemaIssue.isIssue(issue) // => true
* SchemaIssue.isIssue("not an issue") // => false
* ```
*
* @see {@link Issue}
*
* @stability stable
* @category guards
* @since 4.0.0
*/
function isIssue(u) {
	return hasProperty(u, TypeId$10) && u[TypeId$10] === TypeId$10;
}
var IssueNodeImpl = class {
	[TypeId$10] = TypeId$10;
	constructor(input, options) {
		if (options?.reportInput === true && input !== missing) this.input = input;
	}
};
/**
* Constructs a schema issue for a failed refinement check.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Filter$1 = class extends IssueNodeImpl {
	_tag = "Filter";
	/**
	* The filter that failed.
	*/
	filter;
	/**
	* The issue that occurred.
	*/
	issue;
	constructor(filter, issue, input, options) {
		super(input, options);
		this.filter = filter;
		this.issue = issue;
	}
};
/**
* Constructs a schema issue for a failed transformation.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Encoding = class extends IssueNodeImpl {
	_tag = "Encoding";
	/**
	* The schema that caused the issue.
	*/
	ast;
	/**
	* The issue that occurred.
	*/
	issue;
	constructor(ast, issue, input, options) {
		super(input, options);
		this.ast = ast;
		this.issue = issue;
	}
};
/**
* Constructs a schema issue that points to a nested location.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Pointer = class extends IssueNodeImpl {
	_tag = "Pointer";
	/**
	* The path to the location in the input that caused the issue.
	*/
	path;
	/**
	* The issue that occurred.
	*/
	issue;
	constructor(path, issue) {
		super();
		this.path = path;
		this.issue = issue;
	}
};
/**
* Constructs a schema issue for a missing key or tuple index.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const MissingKey = class extends IssueNodeImpl {
	_tag = "MissingKey";
	/**
	* The metadata for the issue.
	*/
	annotations;
	constructor(annotations) {
		super();
		this.annotations = annotations;
	}
};
/**
* Constructs a schema issue for an unexpected key or tuple index.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const UnexpectedKey = class extends IssueNodeImpl {
	_tag = "UnexpectedKey";
	/**
	* The schema that caused the issue.
	*/
	ast;
	constructor(ast, input, options) {
		super(input, options);
		this.ast = ast;
	}
};
/**
* Constructs a schema issue that groups multiple child issues.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Composite = class extends IssueNodeImpl {
	_tag = "Composite";
	/**
	* The schema that caused the issue.
	*/
	ast;
	/**
	* The issues that occurred.
	*/
	issues;
	constructor(ast, issues, input, options) {
		super(input, options);
		this.ast = ast;
		this.issues = issues;
	}
};
/**
* Constructs a schema issue for an input with an invalid runtime type.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const InvalidType = class extends IssueNodeImpl {
	_tag = "InvalidType";
	/**
	* The schema that caused the issue.
	*/
	ast;
	constructor(ast, input, options) {
		super(input, options);
		this.ast = ast;
	}
};
/**
* Constructs a schema issue for a value that violates a constraint.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const InvalidValue = class extends IssueNodeImpl {
	_tag = "InvalidValue";
	/**
	* The metadata for the issue.
	*/
	annotations;
	constructor(annotations, input, options) {
		super(input, options);
		this.annotations = annotations;
	}
};
/**
* Constructs a schema issue for a value that matches no union member.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const AnyOf = class extends IssueNodeImpl {
	_tag = "AnyOf";
	/**
	* The schema that caused the issue.
	*/
	ast;
	/**
	* The issues that occurred.
	*/
	issues;
	constructor(ast, issues, input, options) {
		super(input, options);
		this.ast = ast;
		this.issues = issues;
	}
};
/**
* Constructs a schema issue for a value that matches multiple union members.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const OneOf = class extends IssueNodeImpl {
	_tag = "OneOf";
	/**
	* The schema that caused the issue.
	*/
	ast;
	/**
	* The schemas that were successful.
	*/
	successes;
	constructor(ast, successes, input, options) {
		super(input, options);
		this.ast = ast;
		this.successes = successes;
	}
};
function makeFilterIssue(entry, input, options) {
	if (isIssue(entry)) return entry;
	if (typeof entry === "string") return new InvalidValue({ message: entry }, input, options);
	const inner = typeof entry.issue === "string" ? new InvalidValue({ message: entry.issue }, input, options) : entry.issue;
	return new Pointer(entry.path, inner);
}
/** @internal */
function makeSingle(out, input, options) {
	if (out === void 0) return;
	if (typeof out === "boolean") return out ? void 0 : new InvalidValue(void 0, input, options);
	return makeFilterIssue(out, input, options);
}
/** @internal */
function normalizeFilterOutput(ast, out, input, options) {
	if (Array.isArray(out)) {
		if (!isReadonlyArrayNonEmpty(out)) return;
		return out.length === 1 ? makeFilterIssue(out[0], input, options) : new Composite(ast, map$3(out, (entry) => makeFilterIssue(entry, input, options)), input, options);
	}
	return makeSingle(out, input, options);
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/schema/cause.js
/** @internal */
function getSchemaIssue(cause) {
	let issue;
	for (const reason of cause.reasons) {
		if (!isFailReason(reason) || !isIssue(reason.error)) return;
		issue ??= reason.error;
	}
	return issue;
}
/** @internal */
function getSchemaIssueOrThrow(cause, message) {
	const issue = getSchemaIssue(cause);
	if (issue === void 0) throw new Error(message, { cause });
	return issue;
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/SchemaGetter.js
const makeGetter = (fields) => Object.assign(Object.create(Prototype$1), fields);
const passthrough_$1 = /*#__PURE__*/ makeGetter({ _tag: "Passthrough" });
function passthrough$1() {
	return passthrough_$1;
}
/**
* Creates a getter that applies a pure function to present values.
*
* **When to use**
*
* Use when you need a schema getter for a pure, infallible transformation
* between types.
* - Building encode/decode pairs for `Schema.decodeTo`.
*
* **Details**
*
* - This is the most commonly used constructor.
* - Transforms `Some(e)` to `Some(f(e))` and leaves `None` unchanged.
* - Skips `None` inputs — only called when a value is present.
* - Never fails.
*
* **Example** (Transforming strings to numbers)
*
* ```ts import.meta.vitest
* import { Schema, SchemaGetter } from "effect"
*
* const NumberFromString = Schema.String.pipe(
*   Schema.decodeTo(Schema.Number, {
*     decode: SchemaGetter.transform((s) => Number(s)),
*     encode: SchemaGetter.transform((n) => String(n))
*   })
* )
* Schema.decodeSync(NumberFromString)("42") // => 42
* ```
*
* @see {@link transformEffect} when the transformation returns an `Effect`
* @see {@link transformOptional} when you need to handle `None` inputs
* @see {@link passthrough} when no transformation is needed
*
* @stability stable
* @category transforming
* @since 4.0.0
*/
function transform$2(f) {
	return makeGetter({
		_tag: "Transform",
		transform: f
	});
}
/**
* Coerces any value to a `string` using the global `String()` constructor.
*
* **When to use**
*
* Use when you need a schema getter to coerce a present encoded value to a
* string with `String()`.
*
* **Details**
*
* The getter is pure, never fails, and delegates to `globalThis.String`.
*
* **Example** (Coercing to a string)
*
* ```ts import.meta.vitest
* import { Effect, Option, SchemaGetter } from "effect"
*
* const toString = SchemaGetter.String<number>()
* Effect.runSync(SchemaGetter.run(toString, Option.some(42), {})) // => Option.some("42")
* ```
*
* @see {@link transform} for custom string conversions
*
* @stability stable
* @category converting
* @since 4.0.0
*/
function String$3() {
	return transform$2(globalThis.String);
}
/**
* Coerces any value to a `number` using the global `Number()` constructor.
*
* **When to use**
*
* Use when you need a schema getter to coerce a present encoded value to a
* number with `Number()`.
*
* **Details**
*
* The getter is pure, never fails, and delegates to `globalThis.Number`. It may
* produce `NaN` for non-numeric inputs.
*
* **Example** (Coercing to a number)
*
* ```ts import.meta.vitest
* import { Effect, Option, SchemaGetter } from "effect"
*
* const toNumber = SchemaGetter.Number<string>()
* Effect.runSync(SchemaGetter.run(toNumber, Option.some("42"), {})) // => Option.some(42)
* ```
*
* @see {@link transformEffect} for effectful or validated number parsing
*
* @stability stable
* @category converting
* @since 4.0.0
*/
function Number$3() {
	return transform$2(globalThis.Number);
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/SchemaTransformation.js
/**
* Constructs schema middleware from its decode and encode functions.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Middleware = class extends Class$1 {
	_tag = "Middleware";
	decode;
	encode;
	constructor(decode, encode) {
		super();
		this.decode = decode;
		this.encode = encode;
	}
	flip() {
		return new Middleware(this.encode, this.decode);
	}
};
const TypeId$9 = "~effect/SchemaTransformation/Transformation";
/**
* Constructs a bidirectional schema transformation from its decode and encode getters.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Transformation = class extends Class$1 {
	[TypeId$9] = TypeId$9;
	_tag = "Transformation";
	decode;
	encode;
	constructor(decode, encode) {
		super();
		this.decode = decode;
		this.encode = encode;
	}
	flip() {
		return new Transformation(this.encode, this.decode);
	}
};
/**
* Returns `true` if `u` is a `Transformation` instance.
*
* **When to use**
*
* Use to check whether a value is already a schema transformation before
* wrapping it.
*
* **Details**
*
* - Pure predicate, no side effects.
* - Acts as a TypeScript type guard.
*
* **Example** (Checking a value)
*
* ```ts import.meta.vitest
* import { SchemaTransformation } from "effect"
*
* SchemaTransformation.isTransformation(SchemaTransformation.trim()) // => true
* SchemaTransformation.isTransformation({ decode: null, encode: null }) // => false
* ```
*
* @see {@link Transformation}
* @see {@link makeTransformation}
*
* @stability stable
* @category guards
* @since 4.0.0
*/
function isTransformation(u) {
	return hasProperty(u, TypeId$9) && u[TypeId$9] === TypeId$9;
}
/**
* Constructs a `Transformation` from an object with `decode` and `encode`
* `Getter`s. If the input is already a `Transformation`, returns it as-is.
*
* **When to use**
*
* Use when you already have schema getter instances and want to pair them into
* a schema transformation.
* - You want idempotent wrapping (won't double-wrap).
*
* **Details**
*
* - Returns the input unchanged if it is already a `Transformation`.
*
* **Example** (Wrapping existing getters)
*
* ```ts import.meta.vitest
* import { SchemaGetter, SchemaTransformation } from "effect"
*
* const t = SchemaTransformation.makeTransformation({
*   decode: SchemaGetter.transform<number, string>((s) => Number(s)),
*   encode: SchemaGetter.transform<string, number>((n) => String(n))
* })
* t._tag // => "Transformation"
* ```
*
* @see {@link transform} — simpler constructor from pure functions
* @see {@link transformEffect} — constructor from effectful functions
* @see {@link Transformation}
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
const makeTransformation = (options) => {
	if (isTransformation(options)) return options;
	return new Transformation(options.decode, options.encode);
};
/**
* Creates a `Transformation` from pure (sync, infallible) decode and encode
* functions.
*
* **When to use**
*
* Use when you need an infallible schema transformation that does not require
* Effect services.
*
* **Details**
*
* - Each function receives the input and returns the output directly.
* - Skips `None` inputs (missing keys) — functions are only called on present values.
* - Does not allocate Effects internally; uses optimized sync path.
*
* **Example** (Converting between cents and dollars)
*
* ```ts import.meta.vitest
* import { Schema, SchemaTransformation } from "effect"
*
* const CentsFromDollars = Schema.Number.pipe(
*   Schema.decodeTo(
*     Schema.Number,
*     SchemaTransformation.transform({
*       decode: (dollars) => dollars * 100,
*       encode: (cents) => cents / 100
*     })
*   )
* )
* Schema.decodeSync(CentsFromDollars)(2.5) // => 250
* ```
*
* @see {@link transformEffect} — for fallible or effectful transformations
* @see {@link transformOptional} — for transformations that handle missing keys
* @see {@link passthrough} — when no conversion is needed
*
* @stability stable
* @category transforming
* @since 3.10.0
*/
function transform$1(options) {
	return new Transformation(transform$2(options.decode), transform$2(options.encode));
}
const passthrough_ = /*#__PURE__*/ new Transformation(/*#__PURE__*/ passthrough$1(), /*#__PURE__*/ passthrough$1());
function passthrough() {
	return passthrough_;
}
/**
* Decodes a `string` into a `number` and encodes a `number` back to a
* `string`.
*
* **When to use**
*
* Use when you need a schema transformation to parse numeric strings from APIs,
* form data, or URL parameters.
*
* **Details**
*
* Decoding coerces the string to a number like `Number(s)`. Encoding coerces
* the number to a string like `String(n)`. This does not validate that the
* result is finite; combine with `Schema.Finite` or `Schema.Int` for stricter
* checks.
*
* **Example** (Converting a string to a number)
*
* ```ts import.meta.vitest
* import { Schema, SchemaTransformation } from "effect"
*
* const schema = Schema.String.pipe(
*   Schema.decodeTo(Schema.Number, SchemaTransformation.numberFromString)
* )
* Schema.decodeSync(schema)("42") // => 42
* ```
*
* @see {@link bigintFromString}
* @see {@link transform}
*
* @stability stable
* @category converting
* @since 4.0.0
*/
const numberFromString = /*#__PURE__*/ new Transformation(/*#__PURE__*/ Number$3(), /*#__PURE__*/ String$3());
const isJsonError = (input) => isObject$1(input) && typeof input["message"] === "string";
const decodeJsonError = (input) => {
	const err = Object.hasOwn(input, "cause") ? new Error(input.message, { cause: decodeDefect(input.cause) }) : new Error(input.message);
	if (typeof input.name === "string" && input.name !== "Error") err.name = input.name;
	if (typeof input.stack === "string") err.stack = input.stack;
	return err;
};
const encodeUnknownAsJson = (input) => {
	try {
		const json = formatJson(input);
		return json === void 0 ? format$1(input) : JSON.parse(json);
	} catch {
		return format$1(input);
	}
};
const encodeJsonError = (input, options, encodeDefect) => {
	const encoded = {
		name: input.name,
		message: typeof input.message === "string" ? input.message : ""
	};
	if (options?.includeStack && typeof input.stack === "string") encoded.stack = input.stack;
	if (!options?.excludeCause && input.cause !== void 0) encoded.cause = encodeDefect(input.cause);
	return encoded;
};
const makeEncodeDefect = (options) => {
	const seen = /* @__PURE__ */ new WeakSet();
	const encode = (input) => {
		if (isError(input)) {
			if (seen.has(input)) return "[Circular]";
			seen.add(input);
			const encoded = encodeJsonError(input, options, encode);
			seen.delete(input);
			return encoded;
		}
		return encodeUnknownAsJson(input);
	};
	return encode;
};
const decodeDefect = (input) => isJsonError(input) ? decodeJsonError(input) : input;
/** @internal */
const defectFromJson = (options) => transform$1({
	decode: decodeDefect,
	encode: makeEncodeDefect(options)
});
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/SchemaAST.js
/**
* Represents Effect schemas as runtime trees.
*
* Every `Schema` has an AST made from nodes for declarations, primitives,
* literals, arrays, objects, unions, suspended schemas, checks, annotations,
* encoding links, and parsing context. Most users work with the higher-level
* `Schema` module. Use `SchemaAST` when you need to inspect schema nodes, build
* ASTs programmatically, change encoded or decoded views, collect issues, or
* run low-level schema checks.
*
* @stability stable
* @since 4.0.0
*/
function makeGuard(tag) {
	return (ast) => ast._tag === tag;
}
/**
* Narrows an {@link AST} to {@link Declaration}.
*
* **When to use**
*
* Use to recognize declaration AST nodes before running declaration-specific
* handling.
*
* @see {@link Declaration} for the AST node type narrowed by this guard
*
* @stability stable
* @category guards
* @since 3.10.0
*/
const isDeclaration = /*#__PURE__*/ makeGuard("Declaration");
/**
* Narrows an {@link AST} to {@link Never}.
*
* **When to use**
*
* Use to detect the AST node for a schema that can never match before handling
* other schema variants.
*
* @see {@link Never} for the AST node type narrowed by this guard
* @see {@link never} for the singleton `Never` AST instance
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isNever = /*#__PURE__*/ makeGuard("Never");
/**
* Narrows an {@link AST} to {@link Literal}.
*
* **When to use**
*
* Use to recognize exact string, number, boolean, or bigint literal AST nodes.
*
* @see {@link Literal} for the AST node type narrowed by this guard
* @see {@link LiteralValue} for the values stored by literal nodes
*
* @stability stable
* @category guards
* @since 3.10.0
*/
const isLiteral = /*#__PURE__*/ makeGuard("Literal");
/**
* Narrows an {@link AST} to {@link UniqueSymbol}.
*
* @stability stable
* @category guards
* @since 3.10.0
*/
const isUniqueSymbol = /*#__PURE__*/ makeGuard("UniqueSymbol");
/**
* Narrows an {@link AST} to {@link Arrays}.
*
* **When to use**
*
* Use to recognize array-like AST nodes before reading their element, rest, or
* mutability metadata.
*
* @see {@link Arrays} for the AST node type narrowed by this guard
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isArrays = /*#__PURE__*/ makeGuard("Arrays");
/**
* Narrows an {@link AST} to {@link Objects}.
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isObjects = /*#__PURE__*/ makeGuard("Objects");
/**
* Narrows an {@link AST} to {@link Suspend}.
*
* @stability stable
* @category guards
* @since 3.10.0
*/
const isSuspend = /*#__PURE__*/ makeGuard("Suspend");
/**
* Constructs a {@link Link}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Link = class {
	to;
	transformation;
	constructor(to, transformation) {
		this.to = to;
		this.transformation = transformation;
	}
};
/** @internal */
const defaultParseOptions = {};
/**
* Constructs a {@link Context}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Context = class {
	isOptional;
	isMutable;
	/** Used for constructor default values (e.g. `withConstructorDefault` API) */
	constructorDefault;
	annotations;
	constructor(isOptional, isMutable, constructorDefault = void 0, annotations = void 0) {
		this.isOptional = isOptional;
		this.isMutable = isMutable;
		this.constructorDefault = constructorDefault;
		this.annotations = annotations;
	}
};
const TypeId$8 = "~effect/Schema";
var ASTNodeImpl = class {
	[TypeId$8] = TypeId$8;
	annotations;
	checks;
	encoding;
	context;
	constructor(annotations = void 0, checks = void 0, encoding = void 0, context = void 0) {
		this.annotations = annotations;
		this.checks = checks;
		this.encoding = encoding;
		this.context = context;
	}
	toString() {
		return `<${this._tag}>`;
	}
};
/**
* Constructs a {@link Declaration}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Declaration = class extends ASTNodeImpl {
	_tag = "Declaration";
	typeParameters;
	run;
	encodingChecks;
	/**
	* Parser factory {@link flip} swaps in, so a declaration can behave
	* differently when encoding. `undefined` reuses `run`.
	*/
	encodingRun;
	constructor(typeParameters, run, annotations, checks, encoding, context, encodingChecks, encodingRun) {
		super(annotations, checks, encoding, context);
		this.typeParameters = typeParameters;
		this.run = run;
		this.encodingChecks = encodingChecks;
		this.encodingRun = encodingRun;
	}
	/** @internal */
	getParser() {
		let run;
		return (input, options) => {
			if (input === missing) return missingExit;
			run ??= this.run(this.typeParameters);
			try {
				return run(input, this, options);
			} catch (error) {
				return die(error);
			}
		};
	}
	_rebuild(recur, checks, encodingChecks, run, encodingRun) {
		const tps = mapOrSame(this.typeParameters, recur);
		return tps === this.typeParameters && checks === this.checks && encodingChecks === this.encodingChecks && run === this.run && encodingRun === this.encodingRun ? this : new Declaration(tps, run, this.annotations, checks, void 0, this.context, encodingChecks, encodingRun);
	}
	/** @internal */
	recur(recur) {
		return this._rebuild(recur, this.checks, this.encodingChecks, this.run, this.encodingRun);
	}
	/** @internal */
	flip(recur) {
		return this._rebuild(recur, this.encodingChecks, this.checks, this.encodingRun ?? this.run, this.run);
	}
	/** @internal */
	getExpected() {
		const expected = this.annotations?.expected;
		if (typeof expected === "string") return expected;
		return "<Declaration>";
	}
};
/**
* Constructs a {@link Unknown}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Unknown$1 = class extends ASTNodeImpl {
	_tag = "Unknown";
	/** @internal */
	getParser() {
		return fromRefinement(this, isUnknown);
	}
	/** @internal */
	getExpected() {
		return "unknown";
	}
};
/**
* Provides the singleton {@link Unknown} AST instance.
*
* **When to use**
*
* Use when you need the reusable AST singleton for a schema node that accepts
* every value while keeping parsed values opaque.
*
* @see {@link any} for the singleton that accepts every value as `any`
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const unknown$1 = /*#__PURE__*/ new Unknown$1();
/**
* Constructs a {@link Literal}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Literal$1 = class extends ASTNodeImpl {
	_tag = "Literal";
	literal;
	constructor(literal, annotations, checks, encoding, context) {
		super(annotations, checks, encoding, context);
		if (typeof literal === "number" && !globalThis.Number.isFinite(literal)) throw new Error(`A numeric literal must be finite, got ${format$1(literal)}`);
		this.literal = literal;
	}
	/** @internal */
	getParser() {
		return fromConst(this, this.literal);
	}
	/** @internal */
	matchPart(s, _options) {
		return s === globalThis.String(this.literal) ? this.literal : void 0;
	}
	/** @internal */
	toCodecJson() {
		return typeof this.literal === "bigint" ? literalToString(this) : this;
	}
	/** @internal */
	toCodecStringTree() {
		return typeof this.literal === "string" ? this : literalToString(this);
	}
	/** @internal */
	getExpected() {
		return typeof this.literal === "string" ? JSON.stringify(this.literal) : globalThis.String(this.literal);
	}
};
function literalToString(ast) {
	const literalAsString = globalThis.String(ast.literal);
	return replaceEncoding(ast, [new Link(new Literal$1(literalAsString), new Transformation(transform$2(() => ast.literal), transform$2(() => literalAsString)))]);
}
/**
* Constructs a {@link String}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const String$2 = class extends ASTNodeImpl {
	_tag = "String";
	/** @internal */
	getParser() {
		return fromRefinement(this, isString$1);
	}
	/** @internal */
	matchPart(s, options) {
		const checks = this.checks;
		return checks && !options.disableChecks && collectIssues(checks, s, void 0, this, options) ? void 0 : s;
	}
	/** @internal */
	getExpected() {
		return "string";
	}
};
/**
* Provides the singleton {@link String} AST instance.
*
* **When to use**
*
* Use as the shared `SchemaAST` node for unconstrained JavaScript strings.
*
* @see {@link String} for the AST node class
* @see {@link isString} for narrowing an AST to a string node
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const string$1 = /*#__PURE__*/ new String$2();
/**
* Constructs a {@link Number}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Number$2 = class extends ASTNodeImpl {
	_tag = "Number";
	/** @internal */
	getParser() {
		return fromRefinement(this, isNumber);
	}
	/** @internal */
	matchKey(s, options) {
		return this._match(isStringNumberRegExp, s, options);
	}
	/** @internal */
	matchPart(s, options) {
		return this._match(isStringFiniteRegExp, s, options);
	}
	_match(regexp, s, options) {
		if (!regexp.test(s)) return void 0;
		const value = globalThis.Number(s);
		if (options.disableChecks || !this.checks) return value;
		return collectIssues(this.checks, value, void 0, this, options) ? void 0 : value;
	}
	/** @internal */
	toCodecJson() {
		if (this.checks && (hasCheck(this.checks, "effect/schema/isFinite") || hasCheck(this.checks, "effect/schema/isInt"))) return this;
		return replaceEncoding(this, [numberToJson]);
	}
	/** @internal */
	toCodecStringTree() {
		if (this.toCodecJson() === this) return replaceEncoding(this, [finiteToString]);
		return replaceEncoding(this, [numberToString]);
	}
	/** @internal */
	getExpected() {
		return "number";
	}
};
function hasCheck(checks, id) {
	return checks.some((check) => check.annotations?.representation?.id === id || check._tag === "FilterGroup" && hasCheck(check.checks, id));
}
/**
* Provides the singleton {@link Number} AST instance.
*
* **When to use**
*
* Use when you need the canonical `SchemaAST` node for schemas that accept any
* JavaScript number value.
*
* @see {@link Number} for the AST node class and serialization behavior
* @see {@link Literal} for exact finite numeric literal AST nodes
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const number$3 = /*#__PURE__*/ new Number$2();
/**
* Constructs a {@link Boolean}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Boolean$2 = class extends ASTNodeImpl {
	_tag = "Boolean";
	/** @internal */
	getParser() {
		return fromRefinement(this, isBoolean);
	}
	/** @internal */
	getExpected() {
		return "boolean";
	}
};
/**
* Provides the singleton {@link Boolean} AST instance.
*
* **When to use**
*
* Use to reuse the standard AST node that accepts either `true` or `false` when
* constructing schema ASTs directly.
*
* @see {@link Boolean} for the AST node class
* @see {@link Literal} for exact boolean literal AST nodes
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const boolean$2 = /*#__PURE__*/ new Boolean$2();
/**
* Constructs a {@link Arrays}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Arrays = class extends ASTNodeImpl {
	_tag = "Arrays";
	isMutable;
	elements;
	rest;
	encodingChecks;
	constructor(isMutable, elements, rest, annotations, checks, encoding, context, encodingChecks) {
		super(annotations, checks, encoding, context);
		this.isMutable = isMutable;
		this.elements = elements;
		this.rest = rest;
		this.encodingChecks = encodingChecks;
		let hasOptional = false;
		for (let i = 0; i < elements.length; i++) if (isOptional(elements[i])) hasOptional = true;
		else if (hasOptional) throw new Error("A required element cannot follow an optional element. ts(1257)");
		if (hasOptional && rest.length > 1) throw new Error("A required element cannot follow an optional element. ts(1257)");
		for (let i = 1; i < rest.length; i++) if (isOptional(rest[i])) throw new Error("An optional element cannot follow a rest element. ts(1266)");
	}
	/** @internal */
	getParser(compile, compileField = compile) {
		const ast = this;
		let elements;
		let rest;
		const elementLen = ast.elements.length;
		const tailLen = Math.max(0, ast.rest.length - 1);
		function getParser(tailThreshold, index) {
			if (index < elementLen) return elements[index];
			else if (index >= tailThreshold) return rest[index - tailThreshold + 1];
			return rest[0];
		}
		const finish = (state) => {
			const { input, len, options } = state;
			if (ast.rest.length === 0 && len > elementLen) for (let i = elementLen; i < len; i++) {
				const unexpected = new UnexpectedKey(ast, input[i], options);
				const issue = new Pointer([i], unexpected);
				if (options.errors === "all") {
					if (state.issues) state.issues.push(issue);
					else state.issues = [issue];
				} else return fail$1(new Composite(ast, [issue], input, options));
			}
			if (state.issues) return fail$1(new Composite(ast, state.issues, input, options));
			return succeed(state.output);
		};
		const parse = (input, options) => {
			const len = input.length;
			const state = {
				ast,
				getParser,
				input,
				len,
				tailThreshold: Math.max(elementLen, len - tailLen),
				output: [],
				issues: void 0,
				options
			};
			const end = ast.rest.length === 0 ? elementLen : Math.max(len, elementLen + tailLen);
			const concurrency = options.concurrency === void 0 ? 1 : resolveConcurrency(options.concurrency);
			const eff = concurrency === 1 ? parseArray(state, input, 0, end) : parseArrayConcurrent(state, input, {
				concurrency,
				end
			});
			if (!eff) return finish(state);
			if (effectIsExit(eff)) return flatMapEager(eff, () => finish(state));
			let first = true;
			return suspend(() => {
				if (!first) return parse(input, options);
				first = false;
				return flatMap(eff, () => finish(state));
			});
		};
		return (input, options) => {
			if (input === missing) return missingExit;
			try {
				if (!Array.isArray(input)) return fail$1(new InvalidType(ast, input, options));
				if (!elements) {
					elements = ast.elements.map((ast) => ({
						ast,
						parser: compileField(ast)
					}));
					rest = ast.rest.map((ast) => ({
						ast,
						parser: compileField(ast)
					}));
				}
				return parse(input, options);
			} catch (error) {
				return die(error);
			}
		};
	}
	_rebuild(recur, checks, encodingChecks) {
		const elements = mapOrSame(this.elements, recur);
		const rest = mapOrSame(this.rest, recur);
		return elements === this.elements && rest === this.rest && checks === this.checks && encodingChecks === this.encodingChecks ? this : new Arrays(this.isMutable, elements, rest, this.annotations, checks, void 0, this.context, encodingChecks);
	}
	/** @internal */
	recur(recur) {
		return this._rebuild(recur, this.checks, this.encodingChecks);
	}
	/** @internal */
	flip(recur) {
		return this._rebuild(recur, this.encodingChecks, this.checks);
	}
	/** @internal */
	getExpected() {
		return "array";
	}
};
/** @internal */
function stepArray(s, item, exit, i) {
	if (exit._tag === "Failure") return wrapPropertyKeyIssue(s, s.ast, i, exit);
	const value = exit === sameExit ? item : exit[args];
	if (value !== missing) s.output[i] = value;
	else {
		const p = s.getParser(s.tailThreshold, i);
		if (isOptional(p.ast)) return;
		const issue = new Pointer([i], new MissingKey(p.ast.context?.annotations));
		if (s.options.errors === "all") {
			if (s.issues) s.issues.push(issue);
			else s.issues = [issue];
		} else return fail$2(new Composite(s.ast, [issue], s.input, s.options));
	}
}
const parseArrayOptions = {
	onItem(s, item, i) {
		const value = i < s.len ? item : missing;
		return s.getParser(s.tailThreshold, i).parser(value, s.options);
	},
	step: stepArray
};
/** @internal */
const parseArray = /*#__PURE__*/ iterateEager()(parseArrayOptions);
const parseArrayConcurrent = /*#__PURE__*/ iterateConcurrent()(parseArrayOptions);
const wrapPropertyKeyIssue = (s, ast, key, exit) => {
	if (exit.cause.reasons.length === 0) return exit;
	const issue = getSchemaIssue(exit.cause);
	if (issue === void 0) return failCause$2(map$1(exit.cause, (issue) => new Composite(ast, [new Pointer([key], issue)], s.input, s.options)));
	const pointer = new Pointer([key], issue);
	if (s.options.errors === "all") {
		if (s.issues) s.issues.push(pointer);
		else s.issues = [pointer];
	} else return fail$2(new Composite(ast, [pointer], s.input, s.options));
};
/**
* floating point or integer, with optional exponent
*
* The mantissa alternation keeps rejection linear: `\d*\.?\d+` retries
* every split of a long digit run.
* @internal
*/
const FINITE_PATTERN = "[+-]?(?:\\d+(?:\\.\\d+)?|\\.\\d+)(?:[Ee][+-]?\\d+)?";
/**
* Returns the object keys that match the index signature parameter schema.
* @internal
*/
function getIndexSignatureKeys(input, parameter, options = defaultParseOptions) {
	let stringKeys;
	let symbolKeys;
	function go(parameter) {
		switch (parameter._tag) {
			case "String":
			case "TemplateLiteral": return (stringKeys ??= Object.keys(input)).filter((k) => parameter.matchPart(k, options) !== void 0);
			case "Number": return (stringKeys ??= Object.keys(input)).filter((k) => parameter.matchKey(k, options) !== void 0);
			case "Symbol": return (symbolKeys ??= Object.getOwnPropertySymbols(input)).filter((k) => Object.prototype.propertyIsEnumerable.call(input, k) && parameter.matchKey(k, options) !== void 0);
			case "Union": return [...new Set(parameter.types.flatMap(go))];
			default: return [];
		}
	}
	return go(parameterFromPropertyKey(toEncoded(parameter)));
}
/**
* Constructs a {@link PropertySignature}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const PropertySignature = class {
	name;
	type;
	constructor(name, type) {
		this.name = name;
		this.type = type;
	}
};
function isIndexSignatureParameterSide(ast) {
	switch (ast._tag) {
		case "String":
		case "Number":
		case "Symbol":
		case "TemplateLiteral": return true;
		case "Union": return ast.types.every(isIndexSignatureParameterSide);
		default: return false;
	}
}
function isIndexSignatureParameterEncodedSide(ast) {
	const encoded = getLastEncoding(ast);
	switch (encoded._tag) {
		case "String":
		case "Number":
		case "Symbol":
		case "TemplateLiteral": return true;
		case "Union": return encoded.types.every(isIndexSignatureParameterEncodedSide);
		default: return false;
	}
}
function isIndexSignatureParameter(ast) {
	return isIndexSignatureParameterSide(ast) && isIndexSignatureParameterEncodedSide(ast);
}
/**
* Constructs a {@link IndexSignature}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const IndexSignature = class {
	parameter;
	type;
	constructor(parameter, type) {
		if (!isIndexSignatureParameter(parameter)) throw new Error(`Invalid index signature parameter ${parameter._tag}`);
		this.parameter = parameter;
		this.type = type;
		if (isOptional(type) || type.encoding !== void 0 && isOptional(toEncoded(type))) throw new Error("Cannot use `Schema.optionalKey` or `Schema.optional` with index signatures. Index signatures already allow keys to be absent; use `Schema.UndefinedOr` to allow `undefined` values.");
	}
};
/**
* Constructs a {@link Objects}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Objects = class extends ASTNodeImpl {
	_tag = "Objects";
	propertySignatures;
	indexSignatures;
	encodingChecks;
	constructor(propertySignatures, indexSignatures, annotations, checks, encoding, context, encodingChecks) {
		super(annotations, checks, encoding, context);
		this.propertySignatures = propertySignatures;
		this.indexSignatures = indexSignatures;
		this.encodingChecks = encodingChecks;
	}
	/** @internal */
	getParser(compile, compileField = compile) {
		const ast = this;
		const hasProperties = ast.propertySignatures.length;
		const indexCount = ast.indexSignatures.length;
		if (!hasProperties && !indexCount) return fromRefinement(ast, isNotNullish);
		let properties;
		let indexes;
		const compileMembers = () => {
			if (!properties) {
				properties = ast.propertySignatures.map((ps) => ({
					parser: compileField(ps.type),
					name: ps.name,
					type: ps.type
				}));
				indexes = indexCount ? ast.indexSignatures.map((is) => ({
					is,
					parserKey: compile(parameterFromPropertyKey(is.parameter)),
					parserValue: compileField(is.type)
				})) : void 0;
			}
			return properties;
		};
		const makeFallback = () => {
			const expectedKeys = new Set(ast.propertySignatures.map((ps) => typeof ps.name === "number" ? globalThis.String(ps.name) : ps.name));
			const finishIndex = (s, key, k2, inputValue, exitValue) => {
				if (exitValue._tag === "Failure") return wrapPropertyKeyIssue(s, ast, key, exitValue) ?? void_$1;
				const value = exitValue === sameExit ? inputValue : exitValue[args];
				if (k2 !== missing && value !== missing) {
					if (hasProperties && (expectedKeys.has(key) || expectedKeys.has(typeof k2 === "number" ? globalThis.String(k2) : k2))) return void_$1;
					assignProperty(s.out, k2, value);
				}
				return void_$1;
			};
			const parseIndex = (s, key, index, exitKey) => {
				if (!exitKey) {
					const eff = index.parserKey(key, s.options);
					if (!effectIsExit(eff)) return flatMap(exit(eff), (exit) => parseIndex(s, key, index, exit));
					exitKey = eff;
				}
				if (exitKey._tag === "Failure") return wrapPropertyKeyIssue(s, ast, key, exitKey) ?? void_$1;
				const k2 = exitKey === sameExit ? key : exitKey[args];
				const inputValue = s.input[key];
				const result = index.parserValue(inputValue, s.options);
				return effectIsExit(result) ? finishIndex(s, key, k2, inputValue, result) : flatMap(exit(result), (exit) => finishIndex(s, key, k2, inputValue, exit));
			};
			const parseStringIndex = (s, key, index) => {
				const inputValue = s.input[key];
				const result = index.parserValue(inputValue, s.options);
				return effectIsExit(result) ? finishIndex(s, key, key, inputValue, result) : flatMap(exit(result), (exit) => finishIndex(s, key, key, inputValue, exit));
			};
			const parseIndexes = indexCount ? iterateConcurrent()({
				onItem: (s, [key, index]) => index.is.parameter === string$1 ? parseStringIndex(s, key, index) : parseIndex(s, key, index),
				step: (_s, _item, exit) => exit._tag === "Failure" ? exit : void 0
			}) : void 0;
			return fnUntracedEager(function* (input, options) {
				if (input === missing) return missing;
				if (!(typeof input === "object" && input !== null && !Array.isArray(input))) return yield* fail$1(new InvalidType(ast, input, options));
				compileMembers();
				const record = input;
				const out = {};
				const state = {
					ast,
					input: record,
					out,
					issues: void 0,
					options
				};
				const errorsAllOption = options.errors === "all";
				const onExcessPropertyError = options.onExcessProperty === "error";
				const concurrency = options.concurrency === void 0 ? 1 : resolveConcurrency(options.concurrency);
				const indexKeys = indexCount && onExcessPropertyError ? ast.indexSignatures.map((index) => getIndexSignatureKeys(record, index.parameter, options)) : void 0;
				if (onExcessPropertyError) {
					const coveredKeys = indexKeys ? new Set(expectedKeys) : expectedKeys;
					if (indexKeys) for (const keys of indexKeys) for (const key of keys) coveredKeys.add(key);
					const inputKeys = Reflect.ownKeys(record);
					for (let i = 0; i < inputKeys.length; i++) {
						const key = inputKeys[i];
						if (!coveredKeys.has(key) && Object.prototype.propertyIsEnumerable.call(record, key)) {
							const unexpected = new UnexpectedKey(ast, record[key], options);
							const issue = new Pointer([key], unexpected);
							if (errorsAllOption) {
								if (state.issues) state.issues.push(issue);
								else state.issues = [issue];
								continue;
							} else return yield* fail$1(new Composite(ast, [issue], input, options));
						}
					}
				}
				if (hasProperties) {
					const eff = concurrency === 1 ? parseProperties(state, properties) : parsePropertiesConcurrent(state, properties, { concurrency });
					if (eff) yield* eff;
				}
				if (indexCount && concurrency === 1) for (let i = 0; i < indexCount; i++) {
					const index = indexes[i];
					const parse = index.is.parameter === string$1 ? parseStringIndex : parseIndex;
					const keys = indexKeys?.[i] ?? (index.is.parameter === string$1 ? Object.keys(record) : getIndexSignatureKeys(record, index.is.parameter, options));
					for (let j = 0; j < keys.length; j++) {
						const eff = parse(state, keys[j], index);
						if (!effectIsExit(eff)) yield* eff;
						else if (eff._tag === "Failure") return yield* eff;
					}
				}
				else if (parseIndexes) {
					const keyPairs = empty$1();
					for (let i = 0; i < indexCount; i++) {
						const index = indexes[i];
						const keys = indexKeys?.[i] ?? (index.is.parameter === string$1 ? Object.keys(record) : getIndexSignatureKeys(record, index.is.parameter, options));
						for (let j = 0; j < keys.length; j++) keyPairs.push([keys[j], index]);
					}
					const eff = parseIndexes(state, keyPairs, { concurrency });
					if (eff) yield* eff;
				}
				if (state.issues) return yield* fail$1(new Composite(ast, state.issues, input, options));
				return out;
			});
		};
		if (indexCount) return makeFallback();
		let fallback;
		const resume = (snapshot, index, pending) => {
			const property = properties[index];
			return flatMap(exit(pending), (exit) => {
				const state = {
					...snapshot,
					out: { ...snapshot.out }
				};
				const terminal = stepProperty(state, property, exit);
				if (terminal) return terminal;
				const done = () => succeed(state.out);
				const eff = parseProperties(state, properties.slice(index + 1));
				return eff ? flatMapEager(eff, done) : done();
			});
		};
		return (input, options) => {
			if (input === missing) return missingExit;
			if (options.errors === "all" || options.onExcessProperty !== void 0 || options.concurrency !== void 0 && resolveConcurrency(options.concurrency) !== 1) return (fallback ??= makeFallback())(input, options);
			if (!(typeof input === "object" && input !== null && !Array.isArray(input))) return fail$1(new InvalidType(ast, input, options));
			const props = compileMembers();
			const record = input;
			const out = {};
			const state = {
				ast,
				input: record,
				out,
				issues: void 0,
				options
			};
			try {
				for (let index = 0; index < props.length; index++) {
					const property = props[index];
					const name = property.name;
					const hasKey = hasPropertySignature(record, name);
					const value = hasKey ? record[name] : missing;
					const exit = property.parser(value, options);
					if (!effectIsExit(exit)) return resume(state, index, exit);
					if (exit === sameExit) {
						if (hasKey) assignProperty(out, name, value);
						continue;
					}
					const terminal = stepProperty(state, property, exit);
					if (terminal) return terminal;
				}
			} catch (error) {
				return die(error);
			}
			return succeed(out);
		};
	}
	_rebuild(recur, recurParameter, checks, encodingChecks) {
		const props = mapOrSame(this.propertySignatures, (ps) => {
			const t = recur(ps.type);
			return t === ps.type ? ps : new PropertySignature(ps.name, t);
		});
		const indexes = mapOrSame(this.indexSignatures, (is) => {
			const p = recurParameter(is.parameter);
			const t = recur(is.type);
			return p === is.parameter && t === is.type ? is : new IndexSignature(p, t);
		});
		return props === this.propertySignatures && indexes === this.indexSignatures && checks === this.checks && encodingChecks === this.encodingChecks ? this : new Objects(props, indexes, this.annotations, checks, void 0, this.context, encodingChecks);
	}
	/** @internal */
	flip(recur) {
		return this._rebuild(recur, recur, this.encodingChecks, this.checks);
	}
	/** @internal */
	recur(recur, recurParameter = recur) {
		return this._rebuild(recur, recurParameter, this.checks, this.encodingChecks);
	}
	/** @internal */
	getExpected() {
		if (this.propertySignatures.length === 0 && this.indexSignatures.length === 0) return "object | array";
		return "object";
	}
};
/** @internal */
function stepProperty(s, p, exit) {
	if (exit._tag === "Failure") return wrapPropertyKeyIssue(s, s.ast, p.name, exit);
	if (exit === sameExit) return;
	const value = exit[args];
	if (value !== missing) {
		assignProperty(s.out, p.name, value);
		return;
	}
	delete s.out[p.name];
	if (!isOptional(p.type)) {
		const issue = new Pointer([p.name], new MissingKey(p.type.context?.annotations));
		if (s.options.errors === "all") {
			if (s.issues) s.issues.push(issue);
			else s.issues = [issue];
			return;
		} else return fail$2(new Composite(s.ast, [issue], s.input, s.options));
	}
}
const parsePropertiesOptions = {
	onItem(s, p) {
		if (!hasPropertySignature(s.input, p.name)) return p.parser(missing, s.options);
		const value = s.input[p.name];
		assignProperty(s.out, p.name, value);
		return p.parser(value, s.options);
	},
	step: stepProperty
};
/** @internal */
const parseProperties = /*#__PURE__*/ iterateEager()(parsePropertiesOptions);
const parsePropertiesConcurrent = /*#__PURE__*/ iterateConcurrent()(parsePropertiesOptions);
function combineChecks(a, b) {
	if (!a) return b;
	if (!b) return a;
	return [...a, ...b];
}
/** @internal */
function struct(fields, checks, annotations) {
	return new Objects(Reflect.ownKeys(fields).map((key) => {
		return new PropertySignature(key, fields[key].ast);
	}), [], annotations, checks);
}
/** @internal */
function getAST(self) {
	return self.ast;
}
/** @internal */
function union$1(members, options, checks) {
	return new Union$1(members.map(getAST), options, void 0, checks);
}
const toCandidate = /*#__PURE__*/ memoizeIdempotent((ast) => {
	while (true) {
		if (isSuspend(ast)) return unknown$1;
		const encoding = ast.encoding;
		if (!encoding) return ast.recur?.(toCandidate, identity$1) ?? ast;
		if (encoding.some((link) => link.transformation._tag === "Middleware" && link.transformation.decode !== identity$1)) return unknown$1;
		ast = encoding[encoding.length - 1].to;
	}
});
function getCandidateTypes(ast) {
	switch (ast._tag) {
		case "Null": return ["null"];
		case "Undefined": return ["undefined"];
		case "String":
		case "TemplateLiteral": return ["string"];
		case "Number": return ["number"];
		case "Boolean": return ["boolean"];
		case "Symbol":
		case "UniqueSymbol": return ["symbol"];
		case "BigInt": return ["bigint"];
		case "Arrays": return ["array"];
		case "ObjectKeyword": return [
			"object",
			"array",
			"function"
		];
		case "Objects": return ast.propertySignatures.length || ast.indexSignatures.length ? ["object"] : [
			"string",
			"number",
			"boolean",
			"symbol",
			"bigint",
			"object",
			"array",
			"function"
		];
		case "Enum": return Array.from(new Set(ast.enums.map(([, v]) => typeof v)));
		case "Literal": return [typeof ast.literal];
		case "Union": return Array.from(new Set(ast.types.flatMap(getCandidateTypes)));
		default: return [
			"null",
			"undefined",
			"string",
			"number",
			"boolean",
			"symbol",
			"bigint",
			"object",
			"array",
			"function"
		];
	}
}
/** @internal */
function collectSentinels(ast) {
	switch (ast._tag) {
		default: return [];
		case "Declaration": {
			const s = ast.annotations?.[SENTINELS_ANNOTATION_KEY];
			return Array.isArray(s) ? s : [];
		}
		case "Objects": return ast.propertySignatures.flatMap((ps) => {
			const type = ps.type;
			if (!isOptional(type)) {
				if (isLiteral(type)) return [{
					key: ps.name,
					literal: type.literal
				}];
				if (isUniqueSymbol(type)) return [{
					key: ps.name,
					literal: type.symbol
				}];
			}
			return [];
		});
		case "Arrays": return ast.elements.flatMap((e, i) => {
			if (!isOptional(e)) {
				if (isLiteral(e)) return [{
					key: i,
					literal: e.literal
				}];
				if (isUniqueSymbol(e)) return [{
					key: i,
					literal: e.symbol
				}];
			}
			return [];
		});
		case "Union": {
			if (ast.types.length === 0) return [];
			const members = ast.types.map((type) => collectSentinels(toCandidate(type)));
			return members[0].filter((s) => members.every((sentinels) => sentinels.some((o) => o.key === s.key && o.literal === s.literal)));
		}
		case "Suspend": return collectSentinels(ast.thunk());
	}
}
const candidateIndexCache = /*#__PURE__*/ new WeakMap();
const emptyCandidates = /*#__PURE__*/ Object.freeze([]);
const getRuntimeType = (input) => input === null ? "null" : Array.isArray(input) ? "array" : typeof input;
const hasPropertySignature = (input, key) => key === "__proto__" ? Object.hasOwn(input, key) : key in input;
/** @internal */
function getCandidateIndex(types) {
	let index = candidateIndexCache.get(types);
	if (index) return index;
	let bySentinel;
	let sentinelCandidateCount = 0;
	let otherwise;
	let literalCandidates;
	let onlyLiterals = true;
	const literalOf = [];
	for (let i = 0; i < types.length; i++) {
		const a = types[i];
		const encoded = toCandidate(a);
		if (isNever(encoded)) continue;
		if (isLiteral(encoded) || isUniqueSymbol(encoded)) {
			literalCandidates ??= /* @__PURE__ */ new Map();
			const literal = isLiteral(encoded) ? encoded.literal : encoded.symbol;
			literalOf[i] = literal;
			let arr = literalCandidates.get(literal);
			if (!arr) literalCandidates.set(literal, arr = []);
			arr.push(i);
		} else onlyLiterals = false;
		const sentinels = collectSentinels(encoded);
		if (sentinels.length) {
			bySentinel ??= /* @__PURE__ */ new Map();
			sentinelCandidateCount++;
			for (const { key, literal } of sentinels) {
				let entry = bySentinel.get(key);
				if (!entry) bySentinel.set(key, entry = [/* @__PURE__ */ new Map(), /* @__PURE__ */ new Set()]);
				entry[1].add(i);
				let indexes = entry[0].get(literal);
				if (!indexes) entry[0].set(literal, indexes = /* @__PURE__ */ new Set());
				indexes.add(i);
			}
		} else {
			otherwise ??= {};
			const candidateTypes = getCandidateTypes(encoded);
			for (const t of candidateTypes) (otherwise[t] ??= []).push(i);
		}
	}
	const fallbacks = {};
	const getFallback = (type) => fallbacks[type] ??= Object.freeze(otherwise?.[type] ?? emptyCandidates);
	if (onlyLiterals && literalCandidates) {
		literalCandidates.forEach(Object.freeze);
		index = (input) => literalCandidates.get(input) ?? emptyCandidates;
	} else if (bySentinel?.size === 1 && !otherwise) {
		const [key, [byValue]] = bySentinel.entries().next().value;
		const candidates = /* @__PURE__ */ new Map();
		for (const [literal, indexes] of byValue) candidates.set(literal, Object.freeze(Array.from(indexes)));
		const all = Object.freeze(types.map((_, i) => i));
		index = (input, isConstructor) => {
			if (isObjectKeyword(input)) {
				const value = hasPropertySignature(input, key) ? input[key] : void 0;
				if (value !== void 0) return candidates.get(value) ?? emptyCandidates;
				if (isConstructor) return all;
			}
			return emptyCandidates;
		};
	} else if (bySentinel) {
		let commonSentinel;
		for (const entry of bySentinel) if ((!commonSentinel || entry[1][0].size > commonSentinel[1][0].size) && entry[1][1].size === sentinelCandidateCount) commonSentinel = entry;
		index = (input, isConstructor) => {
			const runtimeType = getRuntimeType(input);
			if (!isObjectKeyword(input)) return getFallback(runtimeType);
			const selected = new Set(otherwise?.[runtimeType]);
			let directKey;
			if (commonSentinel) {
				const [key, [byValue]] = commonSentinel;
				const hasKey = hasPropertySignature(input, key);
				const value = hasKey ? input[key] : void 0;
				if (hasKey && (!isConstructor || value !== void 0)) {
					const match = byValue.get(value);
					if (!match) return getFallback(runtimeType);
					for (const i of match) selected.add(i);
					directKey = key;
				}
			}
			if (directKey === void 0) for (const [key, [byValue, all]] of bySentinel) {
				const hasKey = hasPropertySignature(input, key);
				const value = hasKey ? input[key] : void 0;
				if (hasKey && (!isConstructor || value !== void 0)) {
					const match = byValue.get(value);
					if (match) for (const i of match) selected.add(i);
				} else if (isConstructor) for (const i of all) selected.add(i);
			}
			for (const [key, [byValue, all]] of bySentinel) {
				if (key === directKey) continue;
				const hasKey = hasPropertySignature(input, key);
				const value = hasKey ? input[key] : void 0;
				if (hasKey && (!isConstructor || value !== void 0)) {
					const match = byValue.get(value);
					for (const i of selected) if (all.has(i) && !match?.has(i)) selected.delete(i);
				}
			}
			return Array.from(selected).sort((a, b) => a - b);
		};
	} else index = (input) => {
		const fallback = getFallback(getRuntimeType(input));
		return literalCandidates ? fallback.filter((i) => literalOf[i] === void 0 || literalOf[i] === input) : fallback;
	};
	candidateIndexCache.set(types, index);
	return index;
}
/**
* Constructs a {@link Union}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Union$1 = class extends ASTNodeImpl {
	_tag = "Union";
	types;
	options;
	encodingChecks;
	constructor(types, options, annotations, checks, encoding, context, encodingChecks) {
		super(annotations, checks, encoding, context);
		this.types = types;
		this.options = options;
		this.encodingChecks = encodingChecks;
	}
	/** @internal */
	getParser(compile, compileField) {
		const ast = this;
		const isConstructor = compileField !== void 0;
		const parsers = [];
		const parser = (i) => parsers[i] ??= compile(ast.types[i]);
		let index;
		return (input, options) => {
			if (input === missing) return missingExit;
			const candidates = (index ??= getCandidateIndex(ast.types))(input, isConstructor);
			if (candidates.length === 0) return fail$1(new AnyOf(ast, [], input, options));
			if (candidates.length === 1) {
				const result = parser(candidates[0])(input, options);
				if (result._tag === "Success") return result;
				return effectIsExit(result) ? failSingleUnionCandidate(ast, result.cause, input, options) : catchSingleUnionCandidate(ast, result, input, options);
			}
			return parseUnionCandidates(ast, parser, candidates, input, options);
		};
	}
	_rebuild(recur, checks, encodingChecks) {
		const types = mapOrSame(this.types, recur);
		return types === this.types && checks === this.checks && encodingChecks === this.encodingChecks ? this : new Union$1(types, this.options, this.annotations, checks, void 0, this.context, encodingChecks);
	}
	/** @internal */
	recur(recur) {
		return this._rebuild(recur, this.checks, this.encodingChecks);
	}
	/** @internal */
	flip(recur) {
		return this._rebuild(recur, this.encodingChecks, this.checks);
	}
	/** @internal */
	matchPart(s, options) {
		for (const type of this.types) {
			const out = type.matchPart(s, options);
			if (out !== void 0) return out;
		}
	}
	/** @internal */
	getExpected(getExpected) {
		const expected = this.annotations?.expected;
		if (typeof expected === "string") return expected;
		if (this.types.length === 0) return "never";
		const types = this.types.map((type) => {
			const encoded = toEncoded(type);
			switch (encoded._tag) {
				case "Arrays": {
					const literals = encoded.elements.filter(isLiteral);
					if (literals.length > 0) return `${formatIsMutable(encoded.isMutable)}[ ${literals.map((e) => getExpected(e) + formatIsOptional(e.context?.isOptional)).join(", ")}, ... ]`;
					break;
				}
				case "Objects": {
					const literals = encoded.propertySignatures.filter((ps) => isLiteral(ps.type));
					if (literals.length > 0) return `{ ${literals.map((ps) => `${formatIsMutable(ps.type.context?.isMutable)}${formatPropertyKey(ps.name)}${formatIsOptional(ps.type.context?.isOptional)}: ${getExpected(ps.type)}`).join(", ")}, ... }`;
					break;
				}
			}
			return getExpected(encoded);
		});
		return Array.from(new Set(types)).join(" | ");
	}
};
function failSingleUnionCandidate(ast, cause, input, options) {
	const issue = getSchemaIssue(cause);
	if (!issue) return failCause$2(cause);
	return fail$2(new AnyOf(ast, [issue], input, options));
}
function catchSingleUnionCandidate(ast, result, input, options) {
	return catchCause(result, (cause) => failSingleUnionCandidate(ast, cause, input, options));
}
function parseUnionCandidates(ast, parser, candidates, input, options) {
	const state = {
		ast,
		parser,
		input,
		out: void 0,
		successes: ast.options?.mode === "oneOf" ? [] : void 0,
		issues: void 0,
		options
	};
	const eff = parseUnion(state, candidates);
	if (!eff) {
		if (state.out) return state.out;
		return fail$1(new AnyOf(ast, state.issues ?? [], input, options));
	}
	if (effectIsExit(eff)) return resumeUnion(eff, state);
	let first = true;
	return suspend(() => {
		if (!first) return parseUnionCandidates(ast, parser, candidates, input, options);
		first = false;
		return resumeUnion(eff, state);
	});
}
function resumeUnion(eff, state) {
	return flatMapEager(eff, (_) => {
		if (state.out === sameExit) return succeed$1(state.input);
		if (state.out) return state.out;
		return fail$1(new AnyOf(state.ast, state.issues ?? [], state.input, state.options));
	});
}
const parseUnion = /*#__PURE__*/ iterateEager()({
	onItem(s, i) {
		return s.parser(i)(s.input, s.options);
	},
	step(s, i, exit) {
		if (exit._tag === "Failure") {
			const issue = getSchemaIssue(exit.cause);
			if (issue === void 0) return exit;
			if (s.issues) s.issues.push(issue);
			else s.issues = [issue];
		} else {
			if (s.out && s.successes) {
				s.successes.push(s.ast.types[i]);
				return fail$2(new OneOf(s.ast, s.successes, s.input, s.options));
			}
			s.out = exit;
			if (s.successes) s.successes.push(s.ast.types[i]);
			else return void_$1;
		}
	}
});
const nonFiniteLiterals = /*#__PURE__*/ new Union$1([
	/*#__PURE__*/ new Literal$1("Infinity"),
	/*#__PURE__*/ new Literal$1("-Infinity"),
	/*#__PURE__*/ new Literal$1("NaN")
]);
function formatIsMutable(isMutable) {
	return isMutable ? "" : "readonly ";
}
function formatIsOptional(isOptional) {
	return isOptional ? "?" : "";
}
/**
* Constructs a {@link Filter}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Filter = class extends Class$1 {
	_tag = "Filter";
	run;
	annotations;
	/**
	* Whether the parsing process should be aborted after this check has failed.
	*/
	aborted;
	constructor(run, annotations = void 0, aborted = false) {
		super();
		this.run = run;
		this.annotations = annotations;
		this.aborted = aborted;
	}
	annotate(annotations) {
		return new Filter(this.run, {
			...this.annotations,
			...annotations
		}, this.aborted);
	}
	abort() {
		return new Filter(this.run, this.annotations, true);
	}
	and(other, annotations) {
		return new FilterGroup([this, other], annotations);
	}
};
/**
* Constructs a {@link FilterGroup}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const FilterGroup = class extends Class$1 {
	_tag = "FilterGroup";
	checks;
	annotations;
	constructor(checks, annotations = void 0) {
		super();
		this.checks = checks;
		this.annotations = annotations;
	}
	annotate(annotations) {
		return new FilterGroup(this.checks, {
			...this.annotations,
			...annotations
		});
	}
	and(other, annotations) {
		return new FilterGroup([this, other], annotations);
	}
};
/** @internal */
function makeFilter$1(filter, annotations, aborted = false) {
	return new Filter((input, ast, options) => normalizeFilterOutput(ast, filter(input, ast, options), input, options), annotations, aborted);
}
/** @internal */
function isFinite(annotations) {
	return makeFilter$1((n) => globalThis.Number.isFinite(n), {
		expected: "a finite number",
		representation: {
			id: "effect/schema/isFinite",
			payload: null
		},
		toJsonSchema: () => ({ type: "number" }),
		toCode: () => ({ runtime: "Schema.isFinite()" }),
		arbitraryConstraint: { number: "finite" },
		...annotations
	});
}
const numberToJson = /*#__PURE__*/ new Link(/*#__PURE__*/ new Union$1([/* @__PURE__ */ appendChecks(number$3, [/*#__PURE__*/ isFinite()]), nonFiniteLiterals]), /*#__PURE__*/ new Transformation(/*#__PURE__*/ Number$3(), /*#__PURE__*/ transform$2((n) => globalThis.Number.isFinite(n) ? n : globalThis.String(n))));
/**
* Creates a {@link Filter} that validates strings by running `RegExp.test`.
*
* **When to use**
*
* Use when string validation should be represented as a schema `Filter` backed
* by a regular expression.
*
* **Details**
*
* The filter can be used with `Schema.check` or attached directly to a
* `String` AST node through checks. The regular expression is cloned and its
* `lastIndex` is reset before each test, so global and sticky expressions are
* deterministic and the provided regular expression is not mutated. The
* regular expression source is stored in annotations for serialization and
* arbitrary generation.
*
* **Gotchas**
*
* Arbitrary metadata preserves both `regExp.source` and `regExp.flags`.
* Implementations that cannot consume all flags may still use the source as a
* generation hint because the Schema filter validates every generated value.
* JSON Schema has no way to carry JavaScript regular-expression flags. Unless
* annotations provide `toJsonSchema`, the RegExp constraint is omitted from
* JSON Schema export.
*
* **Example** (Validating an email pattern)
*
* ```ts import.meta.vitest
* import { SchemaAST } from "effect"
*
* const emailFilter = SchemaAST.isPattern(/^[^@]+@[^@]+$/)
* emailFilter.run("alice@example.com", SchemaAST.string, {}) // => undefined
* emailFilter.run("invalid", SchemaAST.string, {})?._tag // => "InvalidValue"
* ```
*
* @see {@link Filter}
* @stability stable
* @category constructors
* @since 4.0.0
*/
function isPattern$1(regExp, annotations) {
	const copy = new globalThis.RegExp(regExp);
	const payload = {
		source: copy.source,
		flags: copy.flags
	};
	return makeFilter$1((s) => {
		copy.lastIndex = 0;
		return copy.test(s);
	}, {
		expected: `a string matching the RegExp ${payload.source}`,
		representation: {
			id: "effect/schema/isPattern",
			payload
		},
		toJsonSchema: () => [{}, true],
		arbitraryConstraint: { patterns: [payload] },
		...annotations
	});
}
const bodyOwners = /*#__PURE__*/ new WeakMap();
function copy(ast, changes) {
	const out = Object.assign(Object.create(Object.getPrototypeOf(ast)), ast, changes);
	if (Reflect.ownKeys(changes).every((key) => key === "context" || key === "encoding")) bodyOwners.set(out, getContextOwner(ast));
	return out;
}
/** @internal */
function getContextOwner(ast) {
	const existing = bodyOwners.get(ast);
	if (existing !== void 0) return existing;
	if (ast.encoding === void 0) return ast;
	const owner = Object.assign(Object.create(Object.getPrototypeOf(ast)), ast, { encoding: void 0 });
	bodyOwners.set(ast, owner);
	return owner;
}
/** @internal */
function replaceEncoding(ast, encoding) {
	return ast.encoding === encoding ? ast : copy(ast, { encoding });
}
/** @internal */
function replaceContext(ast, context) {
	if (ast.context === context) return ast;
	const owner = getContextOwner(ast);
	if (owner.context === context && owner.encoding === ast.encoding) return owner;
	return copy(ast, { context });
}
/** @internal */
function getLastEncoding(ast) {
	return ast.encoding ? getLastEncoding(ast.encoding[ast.encoding.length - 1].to) : ast;
}
/** @internal */
function annotate(ast, annotations) {
	if (ast.checks) {
		const last = ast.checks[ast.checks.length - 1];
		return replaceChecks(ast, append$1(ast.checks.slice(0, -1), last.annotate(annotations)));
	}
	return copy(ast, { annotations: {
		...ast.annotations,
		...annotations
	} });
}
/** @internal */
function replaceChecks(ast, checks) {
	if (ast._tag === "Suspend" && checks) throw new Error("Cannot add checks to Suspend");
	if (ast.checks === checks) return ast;
	return copy(ast, { checks });
}
/** @internal */
function appendChecks(ast, checks) {
	return replaceChecks(ast, combineChecks(ast.checks, checks));
}
/** @internal */
function mapLink(link, f) {
	const to = f(link.to);
	return to === link.to ? link : new Link(to, link.transformation);
}
function updateLastLink(encoding, f) {
	const links = encoding;
	const last = links[links.length - 1];
	const out = mapLink(last, f);
	return out === last ? encoding : append$1(encoding.slice(0, encoding.length - 1), out);
}
/** @internal */
function applyToLastLink(f) {
	return (ast) => ast.encoding ? replaceEncoding(ast, updateLastLink(ast.encoding, f)) : ast;
}
/** @internal */
function applyToSelfOrLastLinkEncodingIdempotent(f, options) {
	function out(ast) {
		if (ast.encoding) {
			const last = ast.encoding[ast.encoding.length - 1];
			return options?.stopAt?.(last) ? ast : replaceEncoding(ast, updateLastLink(ast.encoding, out));
		}
		return f(ast);
	}
	return memoizeIdempotent(out);
}
function appendTransformation(from, transformation, to) {
	const link = new Link(from, transformation);
	return replaceEncoding(to, to.encoding ? [...to.encoding, link] : [link]);
}
function mapOrSame(as, f) {
	let out;
	for (let i = 0; i < as.length; i++) {
		const a = as[i];
		const fa = f(a);
		if (out) out[i] = fa;
		else if (fa !== a) {
			out = new Array(as.length);
			for (let j = 0; j < i; j++) out[j] = as[j];
			out[i] = fa;
		}
	}
	return out ?? as;
}
/** @internal */
function annotateKey(ast, annotations) {
	return replaceContext(ast, ast.context ? new Context(ast.context.isOptional, ast.context.isMutable, ast.context.constructorDefault, {
		...ast.context.annotations,
		...annotations
	}) : new Context(false, false, void 0, annotations));
}
/** @internal */
const optionalKey$1 = /*#__PURE__*/ memoizeIdempotent((ast) => {
	const context = ast.context ? ast.context.isOptional === false ? new Context(true, ast.context.isMutable, ast.context.constructorDefault, ast.context.annotations) : ast.context : new Context(true, false);
	return optionalKeyLastLink(replaceContext(ast, context));
});
const optionalKeyLastLink = /*#__PURE__*/ applyToLastLink(optionalKey$1);
/** @internal */
function withConstructorDefault$1(ast, defaultValue) {
	return replaceContext(ast, ast.context ? new Context(ast.context.isOptional, ast.context.isMutable, defaultValue, ast.context.annotations) : new Context(false, false, defaultValue));
}
/**
* Attaches a `Transformation` to the `to` AST, making it decode from the
* `from` AST and encode back to it.
*
* **Details**
*
* This is the low-level primitive behind `Schema.decodeTo`. It appends a
* {@link Link} to the `to` node's encoding chain.
*
* - Returns a new AST with the same type as `to`.
*
* @see {@link Link}
* @see {@link Encoding}
* @see {@link flip}
* @stability stable
* @category transforming
* @since 4.0.0
*/
function decodeTo$1(from, to, transformation) {
	return appendTransformation(from, transformation, to);
}
function parseParameter(ast) {
	const literals = [];
	const parameters = [];
	function addLiteral(literal) {
		if (!literals.includes(literal)) literals.push(literal);
	}
	function go(ast) {
		switch (ast._tag) {
			case "Literal":
				if (isPropertyKey(ast.literal)) addLiteral(ast.literal);
				return;
			case "UniqueSymbol":
				addLiteral(ast.symbol);
				return;
			case "Enum":
				for (const [, value] of ast.enums) addLiteral(value);
				return;
			case "Never": return;
			case "Union":
				for (let i = 0; i < ast.types.length; i++) go(ast.types[i]);
				return;
			default: parameters.push(ast);
		}
	}
	go(ast);
	return {
		literals,
		parameters
	};
}
/** @internal */
function record$2(key, value) {
	const { literals, parameters: indexSignatures } = parseParameter(key);
	return new Objects(literals.map((literal) => new PropertySignature(literal, value)), indexSignatures.map((parameter) => new IndexSignature(parameter, value)));
}
/**
* Returns `true` if the AST node represents an optional property.
*
* **Details**
*
* Checks `ast.context?.isOptional`. Defaults to `false` when no
* {@link Context} is set.
*
* @see `Schema.optionalKey`
* @see {@link Context}
* @stability stable
* @category predicates
* @since 4.0.0
*/
function isOptional(ast) {
	return ast.context?.isOptional ?? false;
}
function isStructuralCheck(check) {
	return check.annotations?.["~structural"] === true || check._tag === "FilterGroup" && check.checks.every(isStructuralCheck);
}
function extractStructuralChecks(checks) {
	function extract(check) {
		if (isStructuralCheck(check)) return [check];
		return check._tag === "FilterGroup" ? check.checks.flatMap(extract) : [];
	}
	const out = checks.flatMap(extract);
	return isArrayNonEmpty(out) ? out : void 0;
}
function canPreserveEncodingChecks(ast) {
	let preserve = true;
	function visit(child) {
		preserve = preserve && !child.encoding && !isSuspend(child);
		if (preserve && "recur" in child) child.recur(visit);
		return child;
	}
	if ("recur" in ast) ast.recur(visit);
	return preserve;
}
/**
* Strips all encoding transformations from an AST, returning the decoded
* (type-level) representation.
*
* **Details**
*
* - Memoized: same input reference → same output reference.
* - Recursively walks into composite nodes ({@link Arrays}, {@link Objects},
*   {@link Union}, {@link Suspend}).
*
* **Example** (Getting the type AST)
*
* ```ts import.meta.vitest
* import { Schema, SchemaAST } from "effect"
*
* const schema = Schema.NumberFromString
* const typeAst = SchemaAST.toType(schema.ast)
* typeAst._tag // => "Number"
* ```
*
* @see {@link toEncoded}
* @see {@link flip}
* @stability stable
* @category transforming
* @since 4.0.0
*/
const toType = /*#__PURE__*/ memoizeIdempotent((ast) => {
	const owner = getContextOwner(ast);
	if (owner !== ast) {
		const type = toType(owner);
		return type === owner && ast.encoding === void 0 ? ast : replaceContext(type, ast.context);
	}
	const type = "recur" in ast ? ast.recur(toType) : ast;
	if ("encodingChecks" in type && type.encodingChecks) {
		const checks = canPreserveEncodingChecks(ast) ? type.encodingChecks : isArrays(type) || isObjects(type) || isDeclaration(type) && type.typeParameters.length > 0 ? extractStructuralChecks(type.encodingChecks) : void 0;
		return copy(type, {
			encodingChecks: void 0,
			checks: combineChecks(type.checks, checks)
		});
	}
	return type;
});
/**
* Returns the encoded (wire-format) AST by flipping and then stripping
* encodings.
*
* **Details**
*
* Equivalent to `toType(flip(ast))`. This gives you the AST that describes
* the shape of the serialized/encoded data.
*
* - Memoized: same input reference → same output reference.
*
* **Example** (Getting the encoded AST)
*
* ```ts import.meta.vitest
* import { Schema, SchemaAST } from "effect"
*
* const schema = Schema.NumberFromString
* const encodedAst = SchemaAST.toEncoded(schema.ast)
* encodedAst._tag // => "String"
* ```
*
* @see {@link toType}
* @see {@link flip}
* @stability stable
* @category transforming
* @since 4.0.0
*/
const toEncoded = /*#__PURE__*/ memoizeIdempotent((ast) => {
	return toType(flip(ast));
});
function flipEncoding(ast, encoding) {
	const links = encoding;
	const len = links.length;
	const last = links[len - 1];
	const ls = [new Link(flip(replaceEncoding(ast, void 0)), links[0].transformation.flip())];
	for (let i = 1; i < len; i++) ls.unshift(new Link(flip(links[i - 1].to), links[i].transformation.flip()));
	const to = flip(last.to);
	if (to.encoding) return replaceEncoding(to, [...to.encoding, ...ls]);
	else return replaceEncoding(to, ls);
}
/**
* Swaps the decode and encode directions of an AST's {@link Encoding} chain.
*
* **Details**
*
* After flipping, what was decoding becomes encoding and vice versa. This is
* the core operation behind `Schema.encodeEffect` — encoding a value is decoding
* with a flipped SchemaAST.
*
* - Memoized: same input reference → same output reference.
* - Recursively walks composite nodes.
*
* @see {@link toType}
* @see {@link toEncoded}
* @stability stable
* @category transforming
* @since 4.0.0
*/
const flip = /*#__PURE__*/ memoize((ast) => {
	if (ast.encoding) return flipEncoding(ast, ast.encoding);
	const owner = getContextOwner(ast);
	if (owner !== ast) {
		const flipped = flip(owner);
		return flipped === owner ? ast : replaceContext(flipped, ast.context);
	}
	return "flip" in ast ? ast.flip(flip) : "recur" in ast ? ast.recur(flip) : ast;
});
function fromConst(ast, value) {
	const succeed$7 = value === 0 ? sameExit : succeed(value);
	return (input, options) => {
		if (input === missing) return missingExit;
		if (input === value) return succeed$7;
		return fail$1(new InvalidType(ast, input, options));
	};
}
function fromRefinement(ast, refinement) {
	return (input, options) => {
		if (input === missing) return missingExit;
		if (refinement(input)) return sameExit;
		return fail$1(new InvalidType(ast, input, options));
	};
}
/** @internal */
const parameterFromPropertyKey = /*#__PURE__*/ applyToSelfOrLastLinkEncodingIdempotent((ast) => {
	switch (ast._tag) {
		default: return ast;
		case "Number": return ast.toCodecStringTree();
		case "Union": return ast.recur(parameterFromPropertyKey);
	}
});
const isStringFiniteRegExp = /*#__PURE__*/ new globalThis.RegExp(`^${FINITE_PATTERN}$`);
const isStringNumberRegExp = /*#__PURE__*/ new globalThis.RegExp(`^(?:${FINITE_PATTERN}|Infinity|-Infinity|NaN)$`);
/** @internal */
function isStringFinite(annotations) {
	return isPattern$1(isStringFiniteRegExp, {
		expected: "a string representing a finite number",
		representation: {
			id: "effect/schema/isStringFinite",
			payload: null
		},
		toJsonSchema: () => ({ pattern: isStringFiniteRegExp.source }),
		...annotations
	});
}
const finiteString = /*#__PURE__*/ appendChecks(string$1, [/*#__PURE__*/ isStringFinite()]);
const finiteToString = /*#__PURE__*/ new Link(finiteString, numberFromString);
const numberToString = /*#__PURE__*/ new Link(/*#__PURE__*/ new Union$1([finiteString, nonFiniteLiterals]), numberFromString);
/** @internal */
function collectIssues(checks, value, issues, ast, options) {
	for (let i = 0; i < checks.length; i++) {
		const check = checks[i];
		if (check._tag === "FilterGroup") {
			issues = collectIssues(check.checks, value, issues, ast, options);
			if (issues && (options.errors !== "all" || issues[issues.length - 1].filter.aborted)) return issues;
		} else {
			const issue = check.run(value, ast, options);
			if (issue) {
				const filter = new Filter$1(check, issue, value, options);
				if (issues) issues.push(filter);
				else issues = [filter];
				if (options.errors !== "all" || check.aborted) return issues;
			}
		}
	}
	return issues;
}
/** @internal */
function getConstructorDescriptor(ast) {
	if (!isDeclaration(ast)) return void 0;
	const getDescriptor = ast.annotations?.[CONSTRUCTOR_ANNOTATION_KEY];
	return isFunction(getDescriptor) ? getDescriptor(ast.typeParameters) : void 0;
}
function isJsonLeaf(u) {
	return u === null || typeof u === "string" || typeof u === "boolean" || typeof u === "number" && globalThis.Number.isFinite(u);
}
function isStringTreeLeaf(u) {
	return u === void 0 || typeof u === "string";
}
function isTree(u, isLeaf) {
	const cache = /* @__PURE__ */ new WeakMap();
	const stack = [];
	outer: while (true) {
		if (typeof u !== "object" || u === null) {
			if (!isLeaf(u)) return false;
		} else {
			const value = u;
			const cached = cache.get(value);
			if (cached === false) return false;
			if (cached === void 0) {
				const isArray = Array.isArray(value);
				if (!isArray) {
					const prototype = Object.getPrototypeOf(value);
					if (prototype !== null && prototype !== Object.prototype && Object.getPrototypeOf(prototype) !== null) return false;
				}
				cache.set(value, false);
				stack.push({
					value,
					keys: isArray ? value.length : Object.keys(value),
					index: 0
				});
			}
		}
		while (stack.length > 0) {
			const frame = stack[stack.length - 1];
			const keys = frame.keys;
			if (typeof keys === "number") {
				if (frame.index < keys) {
					u = frame.value[frame.index++];
					continue outer;
				}
			} else if (frame.index < keys.length) {
				u = frame.value[keys[frame.index++]];
				continue outer;
			}
			cache.set(frame.value, true);
			stack.pop();
		}
		return true;
	}
}
/**
* Returns true if the value is a JSON value.
*
* When a cyclic reference is detected, returns false.
*
* @internal
*/
function isJson(u) {
	return isTree(u, isJsonLeaf);
}
/** @internal */
const Json$1 = /*#__PURE__*/ new Declaration([], () => (input, ast, options) => isJson(input) ? sameExit : fail$1(new InvalidType(ast, input, options)), {
	representation: {
		id: "effect/schema/Json",
		payload: null
	},
	expected: "JSON value",
	toCodecJson: () => void 0,
	toCodecStringTree: () => unknownToStringTree
});
/**
* Returns true if the value is a StringTree value.
*
* When a cyclic reference is detected, returns false.
*
* @internal
*/
function isStringTree(u) {
	return isTree(u, isStringTreeLeaf);
}
/** @internal */
const unknownToStringTree = /*#__PURE__*/ new Link(/* @__PURE__ */ new Declaration([], () => (input, ast, options) => isStringTree(input) ? sameExit : fail$1(new InvalidType(ast, input, options)), {
	expected: "StringTree",
	toCodecStringTree: () => void 0
}), /*#__PURE__*/ passthrough());
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/schema/interpreter.js
const flatMapTransformation = (result, current, f) => result === sameExit ? f(current) : flatMapEager(result, f);
function compileTransformation(transformation) {
	const apply = compileTransformationUnsafe(transformation);
	return (result, current, options) => {
		try {
			return apply(result, current, options);
		} catch (error) {
			return die(error);
		}
	};
}
function compileTransformationUnsafe(transformation) {
	if (transformation._tag === "Middleware") return (result, current, options) => {
		const transformed = result === sameExit ? transformation.decode(succeed(toOption(current)), options) : transformation.decode(mapEager(result, toOption), options);
		return fromOptionalEffect(transformed);
	};
	const getter = transformation.decode;
	switch (getter._tag) {
		case "Passthrough": return (result, current) => result === sameExit ? succeed(current) : result;
		case "Transform": {
			const transform = (value) => value === missing ? missingExit : succeed(getter.transform(value));
			return (result, current) => flatMapTransformation(result, current, transform);
		}
		case "TransformOptional": {
			const transform = (value) => fromOptionExit(getter.transform(toOption(value)));
			return (result, current) => flatMapTransformation(result, current, transform);
		}
		case "TransformEffect": return (result, current, options) => flatMapTransformation(result, current, (value) => value === missing ? missingExit : getter.transform(value, options));
		case "TransformOptionalEffect": return (result, current, options) => flatMapTransformation(result, current, (value) => fromOptionalEffect(getter.transform(toOption(value), options)));
	}
}
const fromOptionalEffect = (effect) => flatMapEager(effect, fromOptionExit);
/** @internal */
const wrapEncoding = (ast, input, options, effect) => catchCause(effect, (cause) => failCauseSync(() => map$1(cause, (issue) => new Encoding(ast, issue, input, options))));
function makeConstructorParser(descriptor, compile) {
	const transform = compileTransformation(descriptor.link.transformation);
	let sourceParser;
	return (input, options) => {
		if (input === missing) return missingExit;
		if (descriptor.isConstructed(input)) return sameExit;
		const result = (sourceParser ??= compile(descriptor.link.to))(input, options);
		return transform(result, input, options);
	};
}
function withDefault(ast, parser) {
	const defaultValue = ast.context.constructorDefault;
	return (input, options) => {
		if (input !== missing && input !== void 0) return parser(input, options);
		const result = defaultValue;
		if (effectIsExit(result) && result._tag === "Success") {
			const local = parser(result[args], options);
			return local === sameExit ? result : local;
		}
		return flatMapEager(wrapEncoding(ast, input, options, result), (value) => {
			const local = parser(value, options);
			return local === sameExit ? succeed(value) : local;
		});
	};
}
/** @internal */
function compileField(ast, compile) {
	const parser = compile(ast);
	return ast.context?.constructorDefault === void 0 ? parser : withDefault(ast, parser);
}
/** @internal */
function compile(ast, compile, compileField, base, specialize) {
	if (ast._tag === "Declaration") for (const parameter of ast.typeParameters) compile(parameter);
	const descriptor = compileField ? getConstructorDescriptor(ast) : void 0;
	const parser = descriptor ? makeConstructorParser(descriptor, compile) : base ?? ast.getParser(compile, compileField);
	const checks = ast.checks;
	const links = ast.encoding;
	const transformations = links?.map((link) => compileTransformation(link.transformation));
	const encodingChecks = ast.encodingChecks;
	if (!links && !checks && !encodingChecks) return parser;
	let encodingParsers;
	const parseChecks = (input, options) => {
		let result = parser(input, options);
		try {
			if (encodingChecks && !options.disableChecks) {
				if (effectIsExit(result)) {
					if (result._tag === "Success") {
						const output = result === sameExit ? input : result[args];
						if (input !== missing && output !== missing) {
							const issues = collectIssues(encodingChecks, input, void 0, ast, options);
							if (issues) result = fail$1(new Composite(ast, issues, input, options));
						}
					}
				} else result = flatMap(result, (value) => {
					if (input !== missing && value !== missing) {
						const issues = collectIssues(encodingChecks, input, void 0, ast, options);
						if (issues) return fail$1(new Composite(ast, issues, input, options));
					}
					return succeed$1(value);
				});
			}
			if (checks && !options.disableChecks) {
				if (effectIsExit(result)) {
					if (result._tag === "Success") {
						const value = result === sameExit ? input : result[args];
						if (value === missing) return result;
						const issues = collectIssues(checks, value, void 0, ast, options);
						if (issues) result = fail$1(new Composite(ast, issues, value, options));
					}
				} else result = flatMap(result, (value) => {
					if (value !== missing) {
						const issues = collectIssues(checks, value, void 0, ast, options);
						if (issues) return fail$1(new Composite(ast, issues, value, options));
					}
					return succeed$1(value);
				});
			}
			return result;
		} catch (error) {
			return die(error);
		}
	};
	const parseLocal = specialize === void 0 ? parseChecks : specialize(parseChecks);
	if (!links) return parseLocal;
	return (input, options) => {
		const parsers = encodingParsers ??= links.map((link) => compile(link.to));
		let current = input;
		let result = parsers[parsers.length - 1](input, options);
		for (let i = links.length - 1; i >= 0; i--) {
			result = transformations[i](result, current, options);
			if (i !== 0) {
				const next = parsers[i - 1];
				if (result._tag === "Success") {
					current = result[args];
					result = next(current, options);
				} else result = flatMapEager(result, (value) => {
					const nextResult = next(value, options);
					return nextResult === sameExit ? succeed(value) : nextResult;
				});
			}
		}
		if (result._tag === "Success") {
			const value = result[args];
			const local = parseLocal(value, options);
			return local === sameExit ? result : local;
		}
		result = wrapEncoding(ast, input, options, result);
		return flatMapEager(result, (value) => {
			const local = parseLocal(value, options);
			return local === sameExit ? succeed(value) : local;
		});
	};
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/schema/compilerRegistry.js
/** @internal */
const invalid = /*#__PURE__*/ Symbol();
const cache = /*#__PURE__*/ new WeakMap();
const decodeChild = (ast) => resolve(ast).parser;
const makeChild = (ast) => resolve(ast).makeEffect;
const makeField = (ast) => compileField(ast, makeChild);
var InterpretedEntry = class {
	ast;
	constructor(ast) {
		this.ast = ast;
	}
	get decodeEffect() {
		return this.cachedDecodeEffect ??= compile(this.ast, decodeChild);
	}
	get parser() {
		return this.decodeEffect;
	}
	get makeEffect() {
		return this.cachedMakeEffect ??= compile(this.ast, makeChild, makeField);
	}
};
/** @internal */
function resolve(ast) {
	const cached = cache.get(ast);
	if (cached !== void 0) return cached;
	const entry = new InterpretedEntry(ast);
	cache.set(ast, entry);
	return entry;
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/SchemaParser.js
/**
* Runs schemas against real values.
*
* Schema parsers construct values from schema input, check whether a value
* matches a schema, decode encoded input, and encode decoded values back to
* their external form. This module exposes those operations through several
* result styles, including `Effect`, `Promise`, `Exit`, `Option`, `Result`, and
* synchronous functions that throw. It also contains the lower-level runner that
* walks a schema AST and reports schema failures as `SchemaIssue.Issue` values.
*
* @stability stable
* @since 4.0.0
*/
/**
* Creates an effectful maker for the schema's decoded type side.
*
* **When to use**
*
* Use to construct decoded schema values in `Effect` while preserving
* construction failures as `SchemaIssue.Issue` values in the error channel.
*
* **Details**
*
* The returned function accepts constructor input, applies constructor defaults,
* runs type-side validation unless checks are disabled, and fails with a
* `SchemaIssue.Issue` when construction fails.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
function makeEffect(schema) {
	const ast = schema.ast;
	let parser;
	return (input, options) => {
		return (parser ??= runWithCompiler(constructorCompiler, toType(ast)))(input, options?.disableChecks ? options?.parseOptions ? {
			...options.parseOptions,
			disableChecks: true
		} : { disableChecks: true } : options?.parseOptions);
	};
}
/**
* Creates a synchronous maker that returns `Option.some` with the constructed
* value on success, or `Option.none` when construction fails with schema issues.
*
* **When to use**
*
* Use when you need to validate schema constructor input and only care whether
* construction succeeds, without exposing `SchemaIssue.Issue` details.
*
* **Gotchas**
*
* Only causes made entirely of schema issues are converted to `Option.none`.
* Causes that contain defects, interruptions, or asynchronous work at this
* synchronous boundary throw an `Error` whose cause is the underlying `Cause`.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
function makeOption(schema) {
	const parser = makeEffect(schema);
	return (input, options) => {
		const exit = runSyncExit(parser(input, options));
		if (isSuccess(exit)) return some(exit.value);
		getSchemaIssueOrThrow(exit.cause, "Option adapter can only return none for schema issues");
		return none();
	};
}
/**
* Creates a synchronous maker for the schema's decoded type side.
*
* **When to use**
*
* Use to construct decoded schema values synchronously when invalid input
* should throw an `Error` whose cause is `SchemaIssue.Issue`.
*
* **Details**
*
* The returned function constructs a value from constructor input and throws an
* `Error` with the `SchemaIssue.Issue` in its `cause` when construction fails.
* Schema validation failures use the generic message `"Schema validation failed"`.
* Format the `cause` explicitly with `SchemaIssue.makeFormatterDefault()` when
* human-readable details are needed.
*
* **Gotchas**
*
* Causes that contain defects, interruptions, or asynchronous work at this
* synchronous boundary throw an `Error` whose cause is the underlying `Cause`,
* instead of being converted to a schema validation error.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
function make$5(schema) {
	return makeConstructorSync(toType(schema.ast));
}
const getValue = (value) => {
	if (value === missing) return fail$1(new InvalidValue());
	return succeed$1(value);
};
function parserResult(result, input) {
	if (result === sameExit) return succeed$1(input);
	if (!effectIsExit(result)) return flatMapEager(result, getValue);
	return result["~effect/Effect/args"] === missing ? getValue(missing) : result;
}
function runWithCompiler(compiler, ast) {
	let parser;
	return (input, options) => {
		const result = (parser ??= compiler(ast))(input, options ?? defaultParseOptions);
		if (result === sameExit) return succeed$1(input);
		if (!effectIsExit(result)) return flatMapEager(result, getValue);
		return result["~effect/Effect/args"] === missing ? getValue(missing) : result;
	};
}
function runSync(effect, message) {
	const exit = runSyncExit(effect);
	if (isSuccess(exit)) return exit.value;
	const issue = getSchemaIssueOrThrow(exit.cause, message);
	throw new Error("Schema validation failed", { cause: issue });
}
const constructorDefectMessage = "Constructor adapter can only throw schema issues";
function throwConstructorDefect(error) {
	throw new Error(constructorDefectMessage, { cause: die$1(error) });
}
function makeConstructorSync(ast) {
	let entry;
	return (input, options) => constructSync(entry ??= resolve(ast), input, options);
}
function constructSync(entry, input, options) {
	const parseOptions = options?.disableChecks ? options.parseOptions ? {
		...options.parseOptions,
		disableChecks: true
	} : { disableChecks: true } : options?.parseOptions ?? defaultParseOptions;
	const make = entry.make;
	if (make !== void 0 && input !== missing) {
		let output;
		try {
			output = make(input, parseOptions);
		} catch (error) {
			throwConstructorDefect(error);
		}
		if (output !== invalid && output !== missing) return output;
	}
	return runSync(parserResult(entry.makeEffect(input, parseOptions), input), constructorDefectMessage);
}
/**
* Constructs a class instance from its type-side declaration.
*
* When the declaration has no compiled constructor, the fields are validated first
* and the instance is built outside the parser, so stacks captured by the
* class constructor keep the caller's frames.
*
* @internal
*/
function makeClassSync(ast, input, options, construct) {
	const entry = resolve(ast);
	const descriptor = getConstructorDescriptor(ast);
	if (descriptor === void 0 || entry.make !== void 0 || entry.compiledMakeEffect !== void 0) return constructSync(entry, input, options);
	if (descriptor.isConstructed(input)) return input;
	const fields = constructSync((entry.resolve ?? resolve)(descriptor.link.to), input, options);
	try {
		return construct(fields);
	} catch (error) {
		throwConstructorDefect(error);
	}
}
const constructorCompiler = (ast) => resolve(ast).makeEffect;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/schema/make.js
/** @internal */
const TypeId$7 = "~effect/Schema/Schema";
const RebuildOptions = /*#__PURE__*/ Symbol();
const SchemaProto = {
	[TypeId$7]: TypeId$7,
	get make() {
		const value = make$5(this);
		Object.defineProperty(this, "make", {
			value,
			enumerable: true
		});
		return value;
	},
	get makeEffect() {
		const value = makeEffect(this);
		Object.defineProperty(this, "makeEffect", {
			value,
			enumerable: true
		});
		return value;
	},
	get makeOption() {
		const value = makeOption(this);
		Object.defineProperty(this, "makeOption", {
			value,
			enumerable: true
		});
		return value;
	},
	pipe() {
		return pipeArguments(this, arguments);
	},
	annotate(annotations) {
		return this.rebuild(annotate(this.ast, annotations));
	},
	annotateKey(annotations) {
		return this.rebuild(annotateKey(this.ast, annotations));
	},
	check(...checks) {
		return this.rebuild(appendChecks(this.ast, checks));
	},
	rebuild(ast) {
		return make$4(ast, this[RebuildOptions]);
	}
};
/** @internal */
function make$4(ast, options) {
	function Schema() {}
	const self = Object.setPrototypeOf(Schema, SchemaProto);
	if (options && (Object.hasOwn(options, "name") || Object.hasOwn(options, "length") || Object.hasOwn(options, "__proto__"))) Object.defineProperties(self, Object.getOwnPropertyDescriptors({ ...options }));
	else Object.assign(self, options);
	self[RebuildOptions] = options;
	self.ast = ast;
	return self;
}
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Struct.js
/**
* Wraps a plain function as a {@link Lambda} value so it can be used with
* {@link map}, {@link mapPick}, and {@link mapOmit}.
*
* **When to use**
*
* Use to create a typed lambda for struct mapping APIs that need type-level
* input and output tracking.
*
* **Details**
*
* The type parameter `L` encodes both the input and output types at the type
* level, allowing the compiler to track how struct value types change. At
* runtime, the returned value is the same function; `lambda` only adjusts the
* type.
*
* **Example** (Wrapping values in arrays)
*
* ```ts import.meta.vitest
* import { pipe, Struct } from "effect"
*
* interface AsArray extends Struct.Lambda {
*   <A>(self: A): Array<A>
*   readonly "~lambda.out": Array<this["~lambda.in"]>
* }
*
* const asArray = Struct.lambda<AsArray>((a) => [a])
* const result = pipe({ x: 1, y: "hello" }, Struct.map(asArray))
* result // => { x: [1], y: ["hello"] }
* ```
*
* @see {@link Lambda} – the type-level interface
* @see {@link map} – apply a lambda to all struct values
* @stability stable
* @category constructors
* @since 4.0.0
*/
const lambda = (f) => f;
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Schema.js
const TypeId$4 = TypeId$7;
/**
* Creates a schema from an AST (Abstract Syntax Tree) node.
*
* **Details**
*
* This is the fundamental constructor for all schemas in the Effect Schema
* library. It takes an AST node and wraps it in a fully-typed schema that
* preserves all type information and provides the complete schema API.
*
* The `make` function is used internally to create all primitive schemas like
* `String`, `Number`, `Boolean`, etc., as well as more complex schemas. It's
* the bridge between the untyped AST representation and the strongly-typed
* schema.
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
const make$3 = make$4;
/**
* Checks whether a value is a `Schema`.
*
* @stability stable
* @category guards
* @since 3.10.0
*/
function isSchema(u) {
	return hasProperty(u, TypeId$4) && u[TypeId$4] === TypeId$4;
}
/**
* Creates an exact optional key schema for struct fields. Unlike `optional`,
* this creates exact optional properties (not `| undefined`) that can be
* completely omitted from the object.
*
* **Example** (Creating a struct with optional key)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const schema = Schema.Struct({
*   name: Schema.String,
*   age: Schema.optionalKey(Schema.Number)
* })
*
* // Type: { readonly name: string; readonly age?: number }
* type Person = typeof schema["Type"]
* ```
*
* @stability stable
* @category combinators
* @since 4.0.0
*/
const optionalKey = /*#__PURE__*/ lambda((schema) => make$3(optionalKey$1(schema.ast), { schema }));
/**
* Creates a schema for a single literal value (string, number, bigint, boolean, or null).
*
* **Example** (Defining a string literal)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const schema = Schema.Literal("hello")
* // Type: Schema.Literal<"hello">
* Schema.decodeSync(schema)("hello") // => "hello"
* ```
*
* @see {@link Literals} for a schema that represents a union of literals.
* @see {@link tag} for a schema that represents a literal value that can be
* used as a discriminator field in tagged unions and has a constructor default.
* @stability stable
* @category constructors
* @since 3.10.0
*/
function Literal(literal) {
	const out = make$3(new Literal$1(literal), {
		literal,
		transform(to) {
			return out.pipe(decodeTo(Literal(to), {
				decode: transform$2(() => to),
				encode: transform$2(() => literal)
			}));
		}
	});
	return out;
}
/**
* Schema for the `unknown` type. Accepts any value without validation.
*
* **When to use**
*
* Use as a top schema when you need to accept any input while preserving
* TypeScript's `unknown` safety at use sites.
*
* @see {@link Any} for the `any` variant.
* @stability stable
* @category schemas
* @since 3.10.0
*/
const Unknown = /*#__PURE__*/ make$3(unknown$1);
/**
* Schema for `string` values. Validates that the input is `typeof` `"string"`.
*
* @stability stable
* @category schemas
* @since 4.0.0
*/
const String$1 = /*#__PURE__*/ make$3(string$1);
/**
* Schema for `number` values, including `NaN`, `Infinity`, and `-Infinity`.
*
* **Details**
*
* Default JSON serializer:
*
* - Finite numbers are serialized as numbers.
* - Non-finite values are serialized as strings (`"NaN"`, `"Infinity"`, `"-Infinity"`).
*
* @see {@link Finite} for a schema that excludes non-finite values.
* @stability stable
* @category schemas
* @since 4.0.0
*/
const Number$1 = /*#__PURE__*/ make$3(number$3);
/**
* Schema for `boolean` values. Validates that the input is `typeof` `"boolean"`.
*
* **When to use**
*
* Use to validate values that are already JavaScript booleans.
*
* @see {@link BooleanFromBit} for a schema that decodes bit literals `0` or `1` into a boolean
*
* @stability stable
* @category schemas
* @since 4.0.0
*/
const Boolean$1 = /*#__PURE__*/ make$3(boolean$2);
function makeStruct(ast, fields) {
	return make$3(ast, {
		fields,
		mapFields(f, options) {
			const fields = f(this.fields);
			return makeStruct(struct(fields, options?.unsafePreserveChecks ? this.ast.checks : void 0), fields);
		}
	});
}
/**
* Defines a struct schema from a map of field schemas.
*
* **Details**
*
* Each field value is a schema. Use {@link optionalKey} or {@link optional} to
* mark fields as optional, and {@link mutableKey} to mark them as mutable.
*
* The resulting schema's `Type` is a readonly object type with the fields'
* decoded types. The `Encoded` form mirrors the field schemas' encoded types.
* Declared fields may be inherited and are copied to own properties in the
* output. The `__proto__` field is accepted only when it is an own property.
* Parsing does not guarantee that output keys retain their input order.
*
* **Example** (Defining a basic struct)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const Person = Schema.Struct({
*   name: Schema.String,
*   age: Schema.Number,
*   email: Schema.optionalKey(Schema.String)
* })
*
* // { readonly name: string; readonly age: number; readonly email?: string }
* type Person = typeof Person.Type
*
* Schema.decodeUnknownSync(Person)({ name: "Alice", age: 30 }) // => { name: "Alice", age: 30 }
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
function Struct(fields) {
	return makeStruct(struct(fields, void 0), fields);
}
/**
* Defines a record schema whose dynamic properties are selected by a key schema
* and decoded with a value schema.
*
* **Details**
*
* For dynamic keys, the key schema selects matching own properties and the
* value schema decodes or encodes only those selected properties. Checks on
* string, number, symbol, and template literal key schemas narrow which
* properties are selected.
*
* For transformed key schemas, property selection is based on encoded property
* names before the selected key is decoded.
*
* **Gotchas**
*
* Dynamic records reject value schemas wrapped with {@link optionalKey} or
* {@link optional}. Index signatures already allow keys to be absent; use
* {@link UndefinedOr} when existing entries may contain `undefined`. Optional
* value schemas remain valid for finite literal keys, which become declared
* properties.
*
* When decoded or encoded key transformations produce the same property key,
* sequential parsing applies selected own properties in selection order, so
* the later selected property overwrites the earlier value. Dynamic key
* selection always examines own properties. Finite literal and enum keys are
* declared fields and may therefore be inherited. Output key order is not
* guaranteed.
*
* **Example** (Defining a string-keyed record of numbers)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const schema = Schema.Record(Schema.String, Schema.Number)
*
* // { readonly [x: string]: number }
* type R = typeof schema.Type
*
* Schema.decodeUnknownSync(schema)({ a: 1, b: 2 }) // => { a: 1, b: 2 }
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
function Record(key, value) {
	return make$3(record$2(key.ast, value.ast), {
		key,
		value
	});
}
/**
* @category constructors
* @since 4.0.0
*/
const ArraySchema = /*#__PURE__*/ lambda((schema) => make$3(new Arrays(false, [], [schema.ast]), { value: schema }));
function makeUnion(ast, members) {
	return make$3(ast, {
		members,
		mapMembers(f, options) {
			const members = f(this.members);
			return makeUnion(union$1(members, this.ast.options, options?.unsafePreserveChecks ? this.ast.checks : void 0), members);
		}
	});
}
/**
* Creates a union schema from an array of member schemas. Members are tested in
* order; the first match is returned.
*
* **Details**
*
* Optionally, specify `mode`:
* - `"anyOf"` (default) — matches if any member matches.
* - `"oneOf"` — matches if exactly one member matches.
*
* **Example** (Defining a string or number union)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const schema = Schema.Union([Schema.String, Schema.Number])
*
* Schema.decodeUnknownSync(schema)("hello") // => "hello"
* Schema.decodeUnknownSync(schema)(42) // => 42
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
function Union(members, options) {
	return makeUnion(union$1(members, options, void 0), members);
}
/**
* Creates a union schema from an array of literal values.
*
* **Example** (Defining status codes)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const schema = Schema.Literals(["active", "inactive", "pending"])
* Schema.decodeSync(schema)("active") // => "active"
* ```
*
* @see {@link Literal} for a schema that represents a single literal.
* @stability stable
* @category constructors
* @since 4.0.0
*/
function Literals(literals) {
	const members = literals.map(Literal);
	return make$3(union$1(members, void 0, void 0), {
		literals,
		members,
		mapMembers(f) {
			return Union(f(this.members));
		},
		pick(literals) {
			return Literals(literals);
		},
		transform(to) {
			return Union(members.map((member, index) => member.transform(to[index])));
		}
	});
}
function decodeTo(to, transformation) {
	return (from) => {
		return make$3(decodeTo$1(from.ast, to.ast, transformation ? makeTransformation(transformation) : passthrough()), {
			from,
			to
		});
	};
}
/**
* Attaches a constructor default value to a schema field.
*
* **Details**
*
* Constructor defaults are applied only during `make*`, not during decoding or
* encoding. Failures are represented directly as `SchemaIssue.Issue` values.
*
* **Example** (Defining an optional field with a static default)
*
* ```ts import.meta.vitest
* import { Effect, Schema } from "effect"
*
* const MySchema = Schema.Struct({
*   name: Schema.String.pipe(
*     Schema.optionalKey,
*     Schema.withConstructorDefault(Effect.succeed("anonymous"))
*   )
* })
*
* MySchema.make({}).name // => "anonymous"
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
function withConstructorDefault(defaultValue) {
	return (schema) => make$3(withConstructorDefault$1(schema.ast, defaultValue), { schema });
}
/**
* Combines a {@link Literal} schema with {@link withConstructorDefault}, making it ideal
* for discriminator fields in tagged unions. When constructing via `make`, the
* `_tag` field can be omitted and will be filled automatically.
*
* **Example** (Defining a discriminated union tag)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const A = Schema.Struct({ _tag: Schema.tag("A"), value: Schema.Number })
*
* // _tag is optional in make, auto-filled to "A"
* const a = A.make({ value: 42 })
* a // => { _tag: "A", value: 42 }
* ```
*
* @see {@link tagDefaultOmit} to also omit the tag during encoding
* @see {@link TaggedStruct} for a shorthand that adds `_tag` automatically
* @stability stable
* @category constructors
* @since 3.10.0
*/
function tag(literal) {
	return Literal(literal).pipe(withConstructorDefault(succeed$1(literal)));
}
/**
* Creates a struct schema with an automatically populated `_tag` field.
*
* **When to use**
*
* Use to define a tagged union case from a literal tag and a set of fields.
*
* **Details**
*
* When using the `make` method, the `_tag` field is optional and will be
* added automatically. However, when decoding or encoding, the `_tag` field
* must be present in the input.
*
* **Example** (Defining a tagged struct shorthand)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* // Defines a struct with a fixed `_tag` field
* const tagged = Schema.TaggedStruct("A", {
*   a: Schema.String
* })
*
* // This is the same as writing:
* const equivalent = Schema.Struct({
*   _tag: Schema.tag("A"),
*   a: Schema.String
* })
* void tagged
* void equivalent
* ```
*
* **Example** (Accessing the literal value of the tag)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const tagged = Schema.TaggedStruct("A", {
*   a: Schema.String
* })
*
* tagged.fields._tag.schema.literal // => "A"
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
function TaggedStruct(value, fields) {
	return Struct({
		_tag: tag(value),
		...fields
	});
}
/**
* Creates a custom validation filter from a predicate function.
*
* **Details**
*
* The predicate receives the decoded input value, the schema AST, and parse
* options, and returns a `FilterOutput`. Non-success outputs are normalized into
* schema issues. The `annotations` parameter annotates the filter itself; with
* the default formatter, failures use `message` first, `expected` second, and
* `<filter>` when neither is provided.
*
* When `abort` is `true`, parsing stops after this filter fails instead of
* collecting later check failures.
*
* **Example** (Reporting failure at a nested path)
*
* ```ts import.meta.vitest
* import { Result, Schema } from "effect"
*
* const schema = Schema.Struct({ password: Schema.String, confirmPassword: Schema.String }).check(
*   Schema.makeFilter((o) =>
*     o.password === o.confirmPassword
*       ? undefined
*       : { path: ["password"], issue: "password and confirmPassword must match" }
*   )
* )
*
* const result = Schema.decodeUnknownResult(schema)({ password: "123456", confirmPassword: "1234567" })
* const message = Result.isFailure(result) ? result.failure.message : ""
* message // => "password and confirmPassword must match\n  at [\"password\"]"
* ```
*
* **Example** (Reporting multiple failures at once)
*
* ```ts import.meta.vitest
* import { Result, Schema } from "effect"
*
* const schema = Schema.Struct({ a: Schema.Finite, b: Schema.Finite, c: Schema.Finite }).check(
*   Schema.makeFilter((o) => {
*     const issues: Array<Schema.FilterIssue> = []
*     if (o.a > 0) {
*       if (o.b <= 0) issues.push({ path: ["b"], issue: "b must be greater than 0" })
*       if (o.c <= 0) issues.push({ path: ["c"], issue: "c must be greater than 0" })
*     }
*     return issues
*   })
* )
*
* const result = Schema.decodeUnknownResult(schema)({ a: 1, b: 0, c: 0 })
* const message = Result.isFailure(result) ? result.failure.message : ""
* message // => "b must be greater than 0\n  at [\"b\"]\nc must be greater than 0\n  at [\"c\"]"
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const makeFilter = makeFilter$1;
/**
* Validates that a string matches the specified regular expression pattern.
*
* **Details**
*
* JSON Schema:
*
* Unless annotations override `toJsonSchema`, JSON Schema receives a `pattern`
* only when the JavaScript RegExp uses the Unicode flag and its other flags are
* `d`, `g`, or `y`. Sticky patterns are anchored at the start of the string.
*
* Arbitrary:
*
* During arbitrary generation, this applies a `patterns`
* constraint to ensure generated strings match the specified RegExp pattern.
*
* @stability stable
* @category validation
* @since 4.0.0
*/
function isPattern(regExp, annotations) {
	const source = regExp.source;
	const flags = regExp.flags;
	const canExport = /^[dg]*uy?$/.test(flags);
	const runtimeRegExp = flags === "" ? `new globalThis.RegExp(${format$1(source)})` : `new globalThis.RegExp(${format$1(source)}, ${format$1(flags)})`;
	return isPattern$1(regExp, {
		toJsonSchema: () => canExport ? { pattern: flags.endsWith("y") ? `^(?:${source})` : source } : [{}, true],
		toCode: () => ({ runtime: `Schema.isPattern(${runtimeRegExp})` }),
		...annotations
	});
}
/**
* Creates an inclusive or exclusive range check for any ordered type from an
* `Order.Order` instance.
*
* @stability stable
* @category validation
* @since 4.0.0
*/
function makeIsBetween(deriveOptions) {
	const greaterThanOrEqualTo = isGreaterThanOrEqualTo$1(deriveOptions.order);
	const greaterThan = isGreaterThan$1(deriveOptions.order);
	const lessThanOrEqualTo = isLessThanOrEqualTo$1(deriveOptions.order);
	const lessThan = isLessThan$1(deriveOptions.order);
	const formatter = deriveOptions.formatter ?? format$1;
	return (options, annotations) => {
		const gte = options.exclusiveMinimum ? greaterThan : greaterThanOrEqualTo;
		const lte = options.exclusiveMaximum ? lessThan : lessThanOrEqualTo;
		return makeFilter((input) => gte(input, options.minimum) && lte(input, options.maximum), {
			expected: `a value between ${formatter(options.minimum)}${options.exclusiveMinimum ? " (excluded)" : ""} and ${formatter(options.maximum)}${options.exclusiveMaximum ? " (excluded)" : ""}`,
			arbitraryConstraint: {
				order: deriveOptions.order,
				minimum: options.minimum,
				maximum: options.maximum,
				...options.exclusiveMinimum && { exclusiveMinimum: true },
				...options.exclusiveMaximum && { exclusiveMaximum: true }
			},
			...deriveOptions.annotate?.(options),
			...annotations
		});
	};
}
function encodeNumberPayload(number) {
	if (!globalThis.Number.isFinite(number)) throw new globalThis.RangeError(`Expected a finite number, got ${format$1(number)}`);
	return number;
}
/**
* Validates that a number is within a specified range. The range boundaries can
* be inclusive or exclusive based on the provided options.
*
* **Details**
*
* JSON Schema:
*
* This check corresponds to `minimum`/`maximum` or `exclusiveMinimum`/`exclusiveMaximum`
* constraints in JSON Schema, depending on the options provided.
*
* Arbitrary:
*
* During arbitrary generation, this applies `minimum` and
* `maximum` constraints with optional `exclusiveMinimum` and
* `exclusiveMaximum` flags to ensure generated numbers fall within the
* specified range.
*
* @stability stable
* @category validation
* @since 4.0.0
*/
const isBetween = /*#__PURE__*/ makeIsBetween({
	order: Number$4,
	annotate: (options) => {
		const exclusiveMinimum = options.exclusiveMinimum ? true : void 0;
		const exclusiveMaximum = options.exclusiveMaximum ? true : void 0;
		return {
			representation: {
				id: "effect/schema/isBetween",
				payload: {
					minimum: encodeNumberPayload(options.minimum),
					maximum: encodeNumberPayload(options.maximum),
					...exclusiveMinimum && { exclusiveMinimum },
					...exclusiveMaximum && { exclusiveMaximum }
				}
			},
			toJsonSchema: () => ({
				[exclusiveMinimum ? "exclusiveMinimum" : "minimum"]: options.minimum,
				[exclusiveMaximum ? "exclusiveMaximum" : "maximum"]: options.maximum
			}),
			toCode: () => ({ runtime: `Schema.isBetween({ minimum: ${format$1(options.minimum)}, maximum: ${format$1(options.maximum)}, exclusiveMinimum: ${format$1(exclusiveMinimum)}, exclusiveMaximum: ${format$1(exclusiveMaximum)} })` })
		};
	}
});
/**
* Validates that a number is a safe integer (within the safe integer range
* that can be exactly represented in JavaScript).
*
* **Details**
*
* JSON Schema:
*
* This check corresponds to `type: "integer"` with `minimum: Number.MIN_SAFE_INTEGER`
* and `maximum: Number.MAX_SAFE_INTEGER` in JSON Schema.
*
* Arbitrary:
*
* During arbitrary generation, this applies an integer constraint.
*
* @stability stable
* @category validation
* @since 4.0.0
*/
function isInt(annotations) {
	return makeFilter((n) => globalThis.Number.isSafeInteger(n), {
		expected: "an integer",
		representation: {
			id: "effect/schema/isInt",
			payload: null
		},
		toJsonSchema: () => ({
			type: "integer",
			minimum: globalThis.Number.MIN_SAFE_INTEGER,
			maximum: globalThis.Number.MAX_SAFE_INTEGER
		}),
		toCode: () => ({ runtime: "Schema.isInt()" }),
		arbitraryConstraint: { number: "integer" },
		...annotations
	});
}
/**
* Schema for integers, rejecting `NaN`, `Infinity`, and `-Infinity`.
*
* @stability stable
* @category schemas
* @since 3.10.0
*/
const Int = /*#__PURE__*/ Number$1.check(/*#__PURE__*/ isInt());
/**
* Validates that a value has at most the specified length. Works with strings
* and arrays.
*
* **Details**
*
* JSON Schema:
*
* The bound must be finite; it is rounded down and clamped to zero. This check
* corresponds to `maxItems` for arrays. Strings use the same bound for
* `maxLength`, which cannot reject a string accepted by this check because a
* string has no more code points than UTF-16 code units.
*
* Arbitrary:
*
* During arbitrary generation, this applies a `maxLength`
* constraint to ensure generated strings or arrays have at most the required
* length.
*
* @stability stable
* @category validation
* @since 4.0.0
*/
function isMaxLength(maxLength, annotations) {
	maxLength = normalizeCardinality(maxLength);
	return makeFilter((input) => input.length <= maxLength, {
		expected: `a value with a length of at most ${maxLength}`,
		representation: {
			id: "effect/schema/isMaxLength",
			payload: { maxLength }
		},
		toJsonSchema: ({ type }) => type === "string" ? maxLength === 0 ? { maxLength } : [{ maxLength }, true] : type === "array" ? { maxItems: maxLength } : type === void 0 ? [{
			maxLength,
			maxItems: maxLength
		}, true] : [{}, true],
		toCode: () => ({ runtime: `Schema.isMaxLength(${maxLength})` }),
		[STRUCTURAL_ANNOTATION_KEY]: true,
		arbitraryConstraint: { maxLength },
		...annotations
	});
}
function normalizeCardinality(value) {
	if (!globalThis.Number.isFinite(value)) throw new globalThis.RangeError(`Expected a finite number, got ${value}`);
	return Math.max(0, Math.floor(value));
}
const getErrorOptionsKey = (options) => (options?.includeStack === true ? 1 : 0) | (options?.excludeCause === true ? 2 : 0);
const getErrorOptions = (key) => {
	switch (key) {
		case 0: return;
		case 1: return { includeStack: true };
		case 2: return { excludeCause: true };
		case 3: return {
			includeStack: true,
			excludeCause: true
		};
	}
};
const defectSchemaCache = [];
/**
* Schema for unexpected defect values represented as `unknown` with a JSON
* encoded form.
*
* **When to use**
*
* Use when you need a schema for `Cause` defects or other unexpected failures
* whose runtime value may be any value.
*
* **Details**
*
* The encoded side is {@link Json}. During decoding, JSON objects with a string
* `message` property are decoded into JavaScript `Error` values, preserving a
* non-default `name` and any string `stack`. Other JSON values decode
* unchanged.
*
* During encoding, JavaScript `Error` values encode to JSON objects with
* `name`, `message`, and optional `cause` properties. Pass
* `{ includeStack: true }` to include string stack traces in encoded `Error`
* defects, or `{ excludeCause: true }` to omit causes. Other values are
* serialized through Effect's JSON formatter and then parsed back into JSON
* when possible.
*
* **Gotchas**
*
* This schema is for carrying defects across JSON boundaries, not for
* preserving every JavaScript value exactly. Some values cannot round-trip
* unchanged:
*
* - A non-`Error` object such as `{ message: "boom" }` encodes as an
*   error-shaped JSON object and decodes back as an `Error`.
* - JSON serialization normalizes unsupported values. For example,
*   `undefined` array elements encode as `null`, unsupported object properties
*   are omitted, and circular references are dropped.
* - Values that cannot be represented as JSON fall back to Effect's formatted
*   string representation.
*
* @see {@link ErrorInstance} for a schema that only accepts JavaScript `Error` values.
* @stability stable
* @category schemas
* @since 4.0.0
*/
function Defect(options) {
	const key = getErrorOptionsKey(options);
	const cached = defectSchemaCache[key];
	if (cached !== void 0) return cached;
	const schema = Json.pipe(decodeTo(Unknown, defectFromJson(getErrorOptions(key))));
	defectSchemaCache[key] = schema;
	return schema;
}
globalThis.RegExp;
globalThis.URL;
globalThis.File;
globalThis.URLSearchParams;
globalThis.Uint8Array;
const immerable = /*#__PURE__*/ globalThis.Symbol.for("immer-draftable");
const payloadToken = {};
function makeClass(Inherited, identifier, struct$1, annotations, proto) {
	const getClassSchema = getClassSchemaFactory(struct$1, identifier, annotations);
	const ClassTypeId = getClassTypeId(identifier);
	const out = class extends Inherited {
		constructor(...[input, options]) {
			const payload = options?.["~payload"];
			const value = payload?.token === payloadToken ? payload.value : struct$1.make(input ?? {}, options);
			super(value, {
				...options,
				disableChecks: true,
				"~payload": {
					token: payloadToken,
					value
				}
			});
		}
		static [TypeId$4] = TypeId$4;
		get [ClassTypeId]() {
			return ClassTypeId;
		}
		static [immerable] = true;
		static identifier = identifier;
		static fields = struct$1.fields;
		static get ast() {
			return getClassSchema(this).ast;
		}
		static pipe() {
			return pipeArguments(this, arguments);
		}
		static rebuild(ast) {
			return getClassSchema(this).rebuild(ast);
		}
		static make(input, options) {
			return makeClassSync(toType(getClassSchema(this).ast), input ?? {}, options, (fields) => makeClassTrusted(this, fields));
		}
		static makeOption(input, options) {
			return makeOption(getClassSchema(this))(input ?? {}, options);
		}
		static makeEffect(input, options) {
			return getClassSchema(this).makeEffect(input ?? {}, options);
		}
		static annotate(annotations) {
			return this.rebuild(annotate(this.ast, annotations));
		}
		static annotateKey(annotations) {
			return this.rebuild(annotateKey(this.ast, annotations));
		}
		static check(...checks) {
			return this.rebuild(appendChecks(this.ast, checks));
		}
		static extend(identifier) {
			return (schema, annotations) => {
				const extension = isStruct(schema) ? schema : Struct(schema);
				const fields = {
					...struct$1.fields,
					...extension.fields
				};
				const ast = struct(fields, struct$1.ast.checks, { identifier });
				return makeClass(this, identifier, makeStruct(appendChecks(ast, extension.ast.checks), fields), annotations, proto);
			};
		}
		static mapFields(f, options) {
			return struct$1.mapFields(f, options);
		}
	};
	if (proto !== void 0) Object.assign(out.prototype, proto(identifier));
	return out;
}
/**
* Constructs a class instance from fields that were already validated against
* the class struct, without running the constructor's schema pass.
*
* @internal
*/
function makeClassTrusted(self, fields) {
	return new self(fields, { "~payload": {
		token: payloadToken,
		value: fields
	} });
}
function getClassTransformation(self) {
	return new Transformation(transform$2((input) => makeClassTrusted(self, input)), passthrough$1());
}
function getClassTypeId(identifier) {
	return `~effect/Schema/Class/${identifier}`;
}
function getClassSchemaFactory(from, identifier, annotations) {
	let memo;
	return (self) => {
		if (memo !== void 0) return memo;
		const ClassTypeId = getClassTypeId(identifier);
		const isClassValue = (input) => input instanceof self || hasProperty(input, ClassTypeId);
		const transformation = getClassTransformation(self);
		return memo = decodeTo(make$3(new Declaration([from.ast], () => (input, ast, options) => {
			return isClassValue(input) ? succeed$1(input) : fail$1(new InvalidType(ast, input, options));
		}, {
			identifier,
			[CONSTRUCTOR_ANNOTATION_KEY]: ([from]) => ({
				isConstructed: isClassValue,
				link: new Link(from, transformation)
			}),
			toCodec: ([from]) => new Link(from.ast, transformation),
			toEquivalence: ([from]) => from,
			toFormatter: ([from]) => (t) => `${self.identifier}(${from(t)})`,
			[SENTINELS_ANNOTATION_KEY]: collectSentinels(from.ast),
			...annotations
		})), transformation)(from);
	};
}
function isStruct(schema) {
	return isSchema(schema);
}
/**
* Creates a schema-backed error class that can be used as a typed,
* yieldable error in Effect programs. Combines {@link Class} validation with
* the `YieldableError` interface so instances can be yielded directly inside
* `Effect.gen`.
*
* **Example** (Schema-backed error)
*
* ```ts import.meta.vitest
* import { Effect, Schema } from "effect"
*
* class NotFound extends Schema.Error<NotFound>("NotFound")({
*   id: Schema.Number
* }) {}
*
* const program = Effect.gen(function*() {
*   yield* new NotFound({ id: 1 })
* })
* const error = await Effect.runPromise(Effect.flip(program))
* error.id // => 1
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Error$1 = (identifier) => (schema, annotations) => {
	const struct = isStruct(schema) ? schema : Struct(schema);
	return makeClass(Error$3, identifier, struct, annotations, (identifier) => ({ name: identifier }));
};
/**
* Defines a schema-backed yieldable error class with an automatically populated
* `_tag` field.
*
* **When to use**
*
* Use to define typed errors that are schema validated, yielded in `Effect.gen`,
* and matched as tagged union members.
*
* **Example** (Defining a tagged error class)
*
* ```ts import.meta.vitest
* import { Effect, Schema } from "effect"
*
* class NotFound extends Schema.TaggedError<NotFound>()("NotFound", {
*   id: Schema.Number
* }) {}
*
* const program = Effect.gen(function*() {
*   yield* new NotFound({ id: 42 })
* })
* const error = await Effect.runPromise(Effect.flip(program))
* error._tag // => "NotFound"
* error.id // => 42
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
const TaggedError = (identifier) => {
	return (tagValue, schema, annotations) => {
		const struct = isStruct(schema) ? schema.mapFields((fields) => ({
			_tag: tag(tagValue),
			...fields
		}), { unsafePreserveChecks: true }) : TaggedStruct(tagValue, schema);
		return Error$1(identifier ?? tagValue)(struct, annotations);
	};
};
/**
* Schema that accepts and validates any immutable JSON-compatible value.
*
* **Example** (Validating a JSON value)
*
* ```ts import.meta.vitest
* import { Option, Schema } from "effect"
*
* Schema.decodeUnknownOption(Schema.Json)({ key: [1, true, null] }) // => Option.some({ key: [1, true, null] })
* ```
*
* @stability stable
* @category schemas
* @since 4.0.0
*/
const Json = /*#__PURE__*/ make$3(/*#__PURE__*/ annotate(Json$1, { toCode: () => ({
	runtime: "Schema.Json",
	Type: "Schema.Json"
}) }));
const PluginId = String$1.check(isPattern(/^[a-z][a-z0-9-]{0,63}$/));
const PluginLocalId = String$1.check(isPattern(/^[a-z][a-z0-9-]{0,63}$/));
const Label = String$1.check(isMaxLength(200));
const Text = String$1.check(isMaxLength(16384));
const PluginInput = Record(String$1, Json);
const PluginPermission = Literals([
	"orchestration:read",
	"orchestration:operate",
	"settings:write",
	"providers:manage",
	"environment:maintain",
	"diagnostics:read",
	"filesystem:read",
	"filesystem:write",
	"terminal:read",
	"terminal:operate",
	"source-control:write",
	"review:write",
	"access:read",
	"access:write",
	"relay:read",
	"relay:write",
	"preview:operate"
]);
Struct({
	apiVersion: Literal(1),
	id: PluginId,
	name: Label,
	version: Label,
	description: optionalKey(Text)
});
const PluginAction = Struct({
	id: PluginLocalId,
	label: Label,
	command: PluginLocalId,
	input: PluginInput
});
const PluginField = Struct({
	name: PluginLocalId,
	label: Label,
	kind: Literals([
		"text",
		"password",
		"number",
		"boolean",
		"select"
	]),
	value: optionalKey(Union([
		String$1,
		Number$1,
		Boolean$1
	])),
	options: optionalKey(ArraySchema(Struct({
		label: Label,
		value: Label
	})).check(isMaxLength(100))),
	required: optionalKey(Boolean$1)
});
const PluginBlock = Union([
	Struct({
		id: PluginLocalId,
		kind: Literal("text"),
		text: Text
	}),
	Struct({
		id: PluginLocalId,
		kind: Literal("link"),
		label: Label,
		url: Text
	}),
	Struct({
		id: PluginLocalId,
		kind: Literal("actions"),
		actions: ArraySchema(PluginAction).check(isMaxLength(20))
	}),
	Struct({
		id: PluginLocalId,
		kind: Literal("form"),
		label: Label,
		command: PluginLocalId,
		fields: ArraySchema(PluginField).check(isMaxLength(40)),
		input: PluginInput
	}),
	Struct({
		id: PluginLocalId,
		kind: Literal("table"),
		columns: ArraySchema(Label).check(isMaxLength(20)),
		rows: ArraySchema(Struct({
			id: Label,
			cells: ArraySchema(Text).check(isMaxLength(20)),
			actions: ArraySchema(PluginAction).check(isMaxLength(20))
		})).check(isMaxLength(500))
	})
]);
const unique = (values) => new Set(values).size === values.length;
Struct({
	title: Label,
	blocks: ArraySchema(PluginBlock).check(isMaxLength(100))
}).check(makeFilter((view) => unique(view.blocks.map((block) => block.id)) && view.blocks.every((block) => {
	if (block.kind === "form") return unique(block.fields.map((field) => field.name)) && block.fields.every((field) => unique(field.options?.map((option) => option.value) ?? []));
	if (block.kind === "actions") return unique(block.actions.map((action) => action.id));
	if (block.kind === "table") return unique(block.columns) && unique(block.rows.map((row) => row.id)) && block.rows.every((row) => row.cells.length === block.columns.length && unique(row.actions.map((action) => action.id)));
	return true;
})));
Struct({
	id: PluginLocalId,
	title: Label,
	location: Literals(["settings", "usage"]),
	permission: PluginPermission
});
Struct({
	id: PluginLocalId,
	title: Label,
	description: Text,
	permission: PluginPermission,
	/** Agent access is opt-in; secret-bearing commands should leave this false. */
	agent: Boolean$1
});
Struct({
	$t3: Literal("secret"),
	label: Label,
	value: Text
});
var PluginError = class extends TaggedError()("PluginError", {
	operation: String$1,
	cause: Defect()
}) {
	get message() {
		return `Plugin operation failed (${this.operation}).`;
	}
};
Record(PluginLocalId, Union([Struct({
	transport: Literal("stdio"),
	command: Text,
	args: ArraySchema(Text).check(isMaxLength(100)),
	environment: Record(Label, Text)
}), Struct({
	transport: Literals(["http", "sse"]),
	url: Text,
	headers: Record(Label, Text)
})])).check(makeFilter((servers) => Object.keys(servers).length <= 100));
Struct({
	provider: Label,
	instanceId: Label,
	projectDirectory: optionalKey(Text),
	cwd: optionalKey(Text)
});
Int.check(isBetween({
	minimum: 1e3,
	maximum: 39e5
}));
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Latch.js
/**
* Creates a `Latch` synchronously, outside of `Effect`.
*
* **When to use**
*
* Use when you need to allocate a `Latch` synchronously outside an Effect
* workflow.
*
* **Details**
*
* The latch starts closed by default; pass `true` to create it open.
*
* **Example** (Creating a latch unsafely)
*
* ```ts import.meta.vitest
* import { Effect, Fiber, Latch } from "effect"
*
* const latch = Latch.makeUnsafe(false)
* const waiter = latch.await.pipe(Effect.as("opened"))
*
* const program = Effect.gen(function*() {
*   const fiber = yield* Effect.forkChild(waiter)
*   yield* latch.open
*   return yield* Fiber.join(fiber)
* })
*
* await Effect.runPromise(program) // => "opened"
* ```
*
* @see {@link make} for creating a latch inside Effect code
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const makeUnsafe = makeLatchUnsafe;
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/MutableList.js
/**
* Mutable lists for collecting ordered values and draining them from the front.
* A `MutableList<A>` can append values to the end, prepend values to the
* beginning, take one or more values from the front, inspect its contents as an
* array, filter values, remove values, and clear itself. All operations update
* the same list object in place and keep its `length` field current. Taking
* from an empty list returns the `Empty` symbol.
*
* @stability stable
* @since 4.0.0
*/
/**
* Defines the unique symbol used to represent an empty result when taking elements from a MutableList.
* This symbol is returned by `take` when the list is empty, allowing for safe type checking.
*
* **When to use**
*
* Use to detect that `take` returned no element before handling the result as a
* list item.
*
* **Example** (Checking for empty results)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
*
* MutableList.take(list) === MutableList.Empty // => true
* ```
*
* @stability stable
* @category symbols
* @since 4.0.0
*/
const Empty = /*#__PURE__*/ Symbol.for("effect/MutableList/Empty");
/**
* Creates an empty MutableList.
*
* **Example** (Creating an empty mutable list)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
*
* list.length // => 0
* MutableList.append(list, "first")
* MutableList.take(list) // => "first"
* list.length // => 0
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const make$2 = () => ({
	head: void 0,
	tail: void 0,
	length: 0
});
const compactHead = (self) => {
	const head = self.head;
	if (head.offset >= 1024 && head === self.tail && head.mutable && (head.array.length - head.offset) * 8 <= head.offset) self.head = self.tail = {
		array: head.array.slice(head.offset),
		mutable: true,
		offset: 0,
		next: void 0
	};
};
const emptyBucket = () => ({
	array: [],
	mutable: true,
	offset: 0,
	next: void 0
});
/**
* Appends an element to the end of the MutableList.
* This operation is optimized for high-frequency usage.
*
* **Example** (Appending elements)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<number>()
* MutableList.append(list, 1)
* MutableList.append(list, 2)
* MutableList.append(list, 3)
*
* MutableList.toArray(list) // => [1, 2, 3]
* list.length // => 3
* ```
*
* @stability stable
* @category mutations
* @since 2.0.0
*/
const append = (self, message) => {
	if (!self.tail) self.head = self.tail = emptyBucket();
	else if (!self.tail.mutable) {
		self.tail.next = emptyBucket();
		self.tail = self.tail.next;
	}
	self.tail.array.push(message);
	self.length++;
};
/**
* Prepends an element to the beginning of the MutableList.
* This operation is optimized for high-frequency usage.
*
* **Example** (Prepending elements)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
* MutableList.append(list, "last")
* MutableList.prepend(list, "third")
* MutableList.prepend(list, "second")
* MutableList.prepend(list, "first")
*
* MutableList.toArray(list) // => ["first", "second", "third", "last"]
* ```
*
* @stability stable
* @category mutations
* @since 2.0.0
*/
const prepend = (self, message) => {
	self.head = {
		array: [message],
		mutable: true,
		offset: 0,
		next: self.head
	};
	if (!self.tail) self.tail = self.head;
	self.length++;
};
/**
* Removes all elements from the MutableList, resetting it to an empty state.
* This operation is highly optimized and releases all internal memory.
*
* **Example** (Clearing a mutable list)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<number>()
* MutableList.appendAll(list, [1, 2, 3, 4, 5])
*
* MutableList.clear(list)
*
* MutableList.toArray(list) // => []
* list.length // => 0
* MutableList.take(list) === MutableList.Empty // => true
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const clear = (self) => {
	self.head = self.tail = void 0;
	self.length = 0;
};
/**
* Takes up to N elements from the beginning of the MutableList and returns them as an array.
* The taken elements are removed from the list. This operation is optimized for performance
* and includes zero-copy optimizations when possible.
*
* **Details**
*
* Finite fractional values of `n` are rounded down. `NaN` and non-positive
* values leave the list unchanged and return an empty array.
*
* **Example** (Taking batches)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<number>()
* MutableList.appendAll(list, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
*
* MutableList.takeN(list, 3) // => [1, 2, 3]
* MutableList.toArray(list) // => [4, 5, 6, 7, 8, 9, 10]
* list.length // => 7
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const takeN = (self, n) => {
	n = normalize$1(n);
	if (n <= 0 || !self.head) return [];
	n = Math.min(n, self.length);
	if (n === self.length && self.head?.offset === 0 && !self.head.next) {
		const array = self.head.array;
		clear(self);
		return array;
	}
	const array = new Array(n);
	let index = 0;
	let chunk = self.head;
	while (chunk) {
		while (chunk.offset < chunk.array.length) {
			array[index++] = chunk.array[chunk.offset];
			if (chunk.mutable) chunk.array[chunk.offset] = void 0;
			chunk.offset++;
			if (index === n) {
				self.head = chunk.offset === chunk.array.length && chunk.next ? chunk.next : chunk;
				self.length -= n;
				if (self.length === 0) clear(self);
				else compactHead(self);
				return array;
			}
		}
		chunk = chunk.next;
	}
	clear(self);
	return array;
};
/**
* Takes all elements from the MutableList and returns them as an array.
* The list becomes empty after this operation. This is equivalent to takeN(list, list.length).
*
* **Example** (Draining all elements)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
* MutableList.appendAll(list, ["apple", "banana", "cherry"])
*
* MutableList.takeAll(list) // => ["apple", "banana", "cherry"]
* list.length // => 0
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const takeAll$1 = (self) => takeN(self, self.length);
/**
* Takes a single element from the beginning of the MutableList.
* Returns the element if available, or the Empty symbol if the list is empty.
* The taken element is removed from the list.
*
* **Example** (Taking one element)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
* MutableList.appendAll(list, ["first", "second", "third"])
*
* MutableList.take(list) // => "first"
* MutableList.toArray(list) // => ["second", "third"]
* list.length // => 2
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const take = (self) => {
	if (!self.head) return Empty;
	const message = self.head.array[self.head.offset];
	if (self.head.mutable) self.head.array[self.head.offset] = void 0;
	self.head.offset++;
	self.length--;
	if (self.head.offset === self.head.array.length) {
		if (self.head.next) self.head = self.head.next;
		else clear(self);
	} else compactHead(self);
	return message;
};
/**
* Filters the MutableList in place, keeping only elements that satisfy the predicate.
* This operation modifies the list and rebuilds its internal structure for efficiency.
* The predicate receives each element's current index.
*
* **Example** (Filtering in place)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<number>()
* MutableList.appendAll(list, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
*
* MutableList.filter(list, (n) => n % 2 === 0)
*
* MutableList.toArray(list) // => [2, 4, 6, 8, 10]
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const filter = (self, f) => {
	const array = [];
	let chunk = self.head;
	let index = 0;
	while (chunk) {
		for (let i = chunk.offset; i < chunk.array.length; i++) if (f(chunk.array[i], index++)) array.push(chunk.array[i]);
		chunk = chunk.next;
	}
	if (array.length === 0) {
		clear(self);
		return;
	}
	self.head = self.tail = {
		array,
		mutable: true,
		offset: 0,
		next: void 0
	};
	self.length = array.length;
};
/**
* Removes all occurrences of a value from the `MutableList` using JavaScript
* strict equality semantics.
*
* **When to use**
*
* Use when in-place removal should use JavaScript identity/strict equality
* rather than Effect structural equality.
*
* **Details**
*
* The list is modified in place.
*
* **Gotchas**
*
* Values are compared with `!==`, so this does not use Effect structural
* equality.
*
* **Example** (Removing matching values)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
* MutableList.appendAll(list, ["apple", "banana", "apple", "cherry", "apple"])
*
* MutableList.remove(list, "apple")
*
* MutableList.toArray(list) // => ["banana", "cherry"]
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const remove = (self, value) => filter(self, (v) => v !== value);
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/MutableRef.js
const TypeId$3 = "~effect/MutableRef";
const MutableRefProto = {
	[TypeId$3]: TypeId$3,
	...PipeInspectableProto,
	toJSON() {
		return {
			_id: "MutableRef",
			current: toJson(this.current)
		};
	}
};
/**
* Creates a new MutableRef with the specified initial value.
*
* **When to use**
*
* Use to create a synchronous `MutableRef` initialized with a value.
*
* **Example** (Creating mutable refs)
*
* ```ts import.meta.vitest
* import { MutableRef } from "effect"
*
* // Create a counter reference
* const counter = MutableRef.make(0)
*
* MutableRef.get(counter) // => 0
*
* // Create a configuration reference
* const config = MutableRef.make({ debug: false, timeout: 5000 })
*
* MutableRef.get(config) // => { debug: false, timeout: 5000 }
*
* // Create a string reference
* const status = MutableRef.make("idle")
* MutableRef.set(status, "running")
*
* MutableRef.get(status) // => "running"
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const make$1 = (value) => {
	const ref = Object.create(MutableRefProto);
	ref.current = value;
	return ref;
};
/**
* Sets the MutableRef to a new value and returns the reference.
*
* **When to use**
*
* Use when you need an in-place `MutableRef` replacement that returns the same
* `MutableRef`.
*
* **Example** (Setting values)
*
* ```ts import.meta.vitest
* import { MutableRef } from "effect"
*
* const ref = MutableRef.make("initial")
*
* // Set a new value
* MutableRef.set(ref, "updated")
*
* MutableRef.get(ref) // => "updated"
*
* // Chain set operations (since it returns the ref)
* const result = MutableRef.set(ref, "final")
*
* result === ref // => true
* MutableRef.get(ref) // => "final"
*
* // Set complex objects
* const config = MutableRef.make({ debug: false, verbose: false })
* MutableRef.set(config, { debug: true, verbose: true })
*
* MutableRef.get(config) // => { debug: true, verbose: true }
*
* // Pipe-able version
* const setValue = MutableRef.set("new value")
* setValue(ref)
*
* MutableRef.get(ref) // => "new value"
*
* // Useful for state management
* const state = MutableRef.make<"idle" | "loading" | "success" | "error">("idle")
* MutableRef.set(state, "loading")
* // ... perform async operation
* MutableRef.set(state, "success")
*
* MutableRef.get(state) // => "success"
* ```
*
* @stability stable
* @category mutations
* @since 2.0.0
*/
const set = /*#__PURE__*/ dual(2, (self, value) => {
	self.current = value;
	return self;
});
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/PubSub.js
/**
* Broadcasts values from publishers to many subscribers.
*
* Publishers add messages with `publish` or `publishAll`, and each active
* `Subscription` receives its own copy of every accepted message. Unlike a
* queue, subscribers do not compete for messages. This module includes bounded,
* dropping, sliding, and unbounded hubs, optional replay buffers for late
* subscribers, message-taking helpers, capacity and shutdown operations, a
* type guard, and low-level types for custom hub strategies.
*
* @stability stable
* @since 2.0.0
*/
const TypeId$2 = "~effect/PubSub";
const SubscriptionTypeId = "~effect/PubSub/Subscription";
/**
* Creates a PubSub with a custom atomic implementation and strategy.
*
* **Example** (Creating a PubSub with a custom strategy)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.gen(function*() {
*   // Create custom PubSub with specific atomic implementation and strategy
*   const pubsub = yield* PubSub.make<string>({
*     atomicPubSub: () => PubSub.makeAtomicBounded(100),
*     strategy: () => new PubSub.BackPressureStrategy()
*   })
*
*   // Use the created PubSub
*   const published = yield* PubSub.publish(pubsub, "Hello")
*   yield* PubSub.shutdown(pubsub)
*   return published
* })
*
* const actual = await Effect.runPromise(program)
* actual // => true
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const make = (options) => sync(() => makePubSubUnsafe(options.atomicPubSub(), /* @__PURE__ */ new Map(), makeUnsafe$3(), makeUnsafe(false), make$1(false), options.strategy(), make$1(none())));
/**
* Creates an unbounded `PubSub`.
*
* **Example** (Creating an unbounded PubSub)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.scoped(Effect.gen(function*() {
*   // Create unbounded PubSub
*   const pubsub = yield* PubSub.unbounded<string>()
*
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Can publish unlimited messages
*   for (let i = 0; i < 3; i++) {
*     yield* PubSub.publish(pubsub, `message-${i}`)
*   }
*
*   return yield* PubSub.takeAll(subscription)
* }))
*
* const actual = await Effect.runPromise(program)
* actual // => ["message-0", "message-1", "message-2"]
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const unbounded = (options) => make({
	atomicPubSub: () => makeAtomicUnbounded(options),
	strategy: () => new DroppingStrategy()
});
/**
* Creates an unbounded atomic PubSub implementation with optional replay buffer.
*
* **When to use**
*
* Use to create the low-level storage layer for a custom `PubSub` whose active
* subscribers may retain an unbounded number of pending messages.
*
* **Gotchas**
*
* Messages published while subscribers are active can be retained without a
* capacity limit until those subscribers take them or unsubscribe.
*
* @see {@link makeAtomicBounded} for a bounded atomic implementation that enforces capacity
* @see {@link make} for wrapping an atomic implementation with a delivery strategy
* @see {@link unbounded} for the high-level effectful constructor for unbounded `PubSub` values
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const makeAtomicUnbounded = (options) => {
	const replay = options?.replay;
	return new UnboundedPubSub(replay && replay > 0 ? new ReplayBuffer(Math.ceil(replay)) : void 0);
};
/**
* Shuts down the `PubSub`, interrupting suspended publishers and subscribers
* and finalizing active subscriptions.
*
* **Details**
*
* After shutdown, `publish` and `publishAll` succeed with `false`,
* `publishUnsafe` returns `false`, and subscription operations such as `take`
* interrupt.
*
* **Example** (Shutting down a PubSub)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<string>(1)
*
*   // Shutdown the PubSub
*   yield* PubSub.shutdown(pubsub)
*
*   const isShutdown = yield* PubSub.isShutdown(pubsub)
*
*   // Publishing after shutdown returns false
*   const published = yield* PubSub.publish(pubsub, "msg1")
*   return { isShutdown, published }
* })
*
* const actual = await Effect.runPromise(program)
* actual // => { isShutdown: true, published: false }
* ```
*
* @stability stable
* @category lifecycle
* @since 2.0.0
*/
const shutdown = (self) => uninterruptible(withFiber((fiber) => {
	set(self.shutdownFlag, true);
	return close(self.scope, interrupt$1(fiber.id)).pipe(andThen(self.strategy.shutdown), when(self.shutdownHook.open), asVoid);
}));
/**
* Publishes a message to the `PubSub` as an `Effect`, returning whether the
* message was accepted.
*
* **When to use**
*
* Use when you need to publish from effectful code and let the configured
* PubSub strategy handle surplus messages.
*
* **Details**
*
* The effect succeeds with `false` if the `PubSub` is shut down. If the message
* cannot be accepted immediately, the configured strategy decides how surplus
* messages are handled.
*
* **Example** (Publishing a message)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.scoped(Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<string>(10)
*
*   // Publish a message
*   const published = yield* PubSub.publish(pubsub, "Hello World")
*
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   yield* PubSub.publish(pubsub, "Hello")
*   const message = yield* PubSub.take(subscription)
*   return { published, message }
* }))
*
* const actual = await Effect.runPromise(program)
* actual // => { published: true, message: "Hello" }
* ```
*
* @see {@link publishUnsafe} for a synchronous non-blocking attempt that does not run effectful surplus handling
*
* @stability stable
* @category publishing
* @since 2.0.0
*/
const publish = /*#__PURE__*/ dual(2, (self, value) => suspend(() => {
	if (self.shutdownFlag.current || isSome(self.ended.current)) return succeed$1(false);
	if (self.pubsub.publish(value)) {
		self.strategy.completeSubscribersUnsafe(self.pubsub, self.subscribers);
		return succeed$1(true);
	}
	return self.strategy.handleSurplus(self.pubsub, self.subscribers, [value], self.shutdownFlag, self.ended);
}));
/**
* Subscribes to receive messages from the `PubSub`. The resulting subscription can
* be evaluated multiple times within the scope to take a message from the `PubSub`
* each time.
*
* **Example** (Subscribing to messages)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<string>(10)
*
*   // Subscribe within a scope for automatic cleanup
*   const first = yield* Effect.scoped(Effect.gen(function*() {
*     const subscription = yield* PubSub.subscribe(pubsub)
*
*     // Publish some messages
*     yield* PubSub.publish(pubsub, "Hello")
*     yield* PubSub.publish(pubsub, "World")
*
*     // Take messages one by one
*     const msg1 = yield* PubSub.take(subscription)
*     const msg2 = yield* PubSub.take(subscription)
*
*     // Subscription is automatically cleaned up when scope exits
*     return [msg1, msg2]
*   }))
*
*   const second = yield* Effect.scoped(Effect.gen(function*() {
*     const sub1 = yield* PubSub.subscribe(pubsub)
*     const sub2 = yield* PubSub.subscribe(pubsub)
*
*     // Multiple subscribers can receive the same messages
*     yield* PubSub.publish(pubsub, "Broadcast")
*
*     return yield* Effect.all([
*       PubSub.take(sub1),
*       PubSub.take(sub2)
*     ])
*   }))
*   return [first, second]
* })
*
* const actual = await Effect.runPromise(program)
* actual // => [["Hello", "World"], ["Broadcast", "Broadcast"]]
* ```
*
* @stability stable
* @category subscriptions
* @since 2.0.0
*/
const subscribe = (self) => uninterruptible(contextWith((services) => {
	const localScope = get(services, Scope);
	const scope = forkUnsafe(self.scope);
	const subscription = makeSubscriptionUnsafe(self.pubsub, self.subscribers, self.strategy, self.ended);
	return addFinalizer$1(scope, unsubscribe(subscription)).pipe(andThen(addFinalizerExit(localScope, (exit) => close(scope, exit))), as(subscription));
}));
const unsubscribe = (self) => uninterruptible(withFiber((state) => {
	set(self.shutdownFlag, true);
	return forEach(takeAll$1(self.pollers), (d) => interruptWith(d, state.id), {
		discard: true,
		concurrency: "unbounded"
	}).pipe(tap(() => sync(() => {
		self.subscribers.delete(self.subscription);
		self.subscription.unsubscribe();
		self.replayWindow.close();
		self.strategy.onPubSubEmptySpaceUnsafe(self.pubsub, self.subscribers);
	})), when(self.shutdownHook.open), asVoid);
}));
/**
* Takes all available messages from the subscription, suspending if no items
* are available.
*
* **Example** (Taking all available messages)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.scoped(Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<string>(10)
*
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Publish multiple messages
*   yield* PubSub.publishAll(pubsub, ["msg1", "msg2", "msg3"])
*
*   // Take all available messages at once
*   return yield* PubSub.takeAll(subscription)
* }))
*
* const actual = await Effect.runPromise(program)
* actual // => ["msg1", "msg2", "msg3"]
* ```
*
* @stability stable
* @category subscriptions
* @since 4.0.0
*/
const takeAll = (self) => suspend(function loop(value) {
	if (self.shutdownFlag.current) return interrupt;
	let as = self.pollers.length === 0 ? self.subscription.pollUpTo(Number.POSITIVE_INFINITY) : [];
	if (value) as = value.concat(as);
	self.strategy.onPubSubEmptySpaceUnsafe(self.pubsub, self.subscribers);
	if (self.replayWindow.remaining > 0) return succeed$1(self.replayWindow.takeAll().concat(as));
	else if (!isArrayNonEmpty(as)) return flatMap(pollForItem(self), (item) => loop([item]));
	return succeed$1(as);
});
const pollForItem = (self) => callback((resume) => {
	if (self.shutdownFlag.current) return resume(interrupt);
	if (isSome(self.ended.current)) {
		const message = self.pollers.length === 0 ? self.subscription.poll() : Empty;
		if (message !== Empty) {
			self.strategy.onPubSubEmptySpaceUnsafe(self.pubsub, self.subscribers);
			return resume(succeed$1(message));
		}
		return resume(succeed$1(self.ended.current.value));
	}
	const deferred = makeUnsafe$4();
	let set = self.subscribers.get(self.subscription);
	if (!set) {
		set = /* @__PURE__ */ new Set();
		self.subscribers.set(self.subscription, set);
	}
	set.add(self.pollers);
	append(self.pollers, deferred);
	self.strategy.completePollersUnsafe(self.pubsub, self.subscribers, self.subscription, self.pollers);
	if (deferred.effect) return resume(deferred.effect);
	deferred.resumes = [resume];
	return sync(() => remove(self.pollers, deferred));
});
const AbsentValue = /*#__PURE__*/ Symbol.for("effect/PubSub/AbsentValue");
const addSubscribers = (subscribers, subscription, pollers) => {
	if (!subscribers.has(subscription)) subscribers.set(subscription, /* @__PURE__ */ new Set());
	subscribers.get(subscription).add(pollers);
};
const removeSubscribers = (subscribers, subscription, pollers) => {
	if (!subscribers.has(subscription)) return;
	const set = subscribers.get(subscription);
	set.delete(pollers);
	if (set.size === 0) subscribers.delete(subscription);
};
const makeSubscriptionUnsafe = (pubsub, subscribers, strategy, ended) => new SubscriptionImpl(pubsub, subscribers, pubsub.subscribe(), make$2(), makeUnsafe(false), make$1(false), strategy, pubsub.replayWindow(), ended);
var UnboundedPubSub = class {
	publisherHead = {
		value: AbsentValue,
		replayIndex: void 0,
		subscribers: 0,
		next: null
	};
	publisherTail = this.publisherHead;
	publisherIndex = 0;
	subscribersIndex = 0;
	capacity = Number.MAX_SAFE_INTEGER;
	replayBuffer;
	constructor(replayBuffer) {
		this.replayBuffer = replayBuffer;
	}
	replayWindow() {
		return this.replayBuffer ? new ReplayWindowImpl(this.replayBuffer) : emptyReplayWindow;
	}
	isEmpty() {
		return this.publisherHead === this.publisherTail;
	}
	isFull() {
		return false;
	}
	size() {
		return this.publisherIndex - this.subscribersIndex;
	}
	publish(value) {
		const replayIndex = this.replayBuffer?.offer(value);
		const subscribers = this.publisherTail.subscribers;
		if (subscribers !== 0) {
			const node = {
				value,
				replayIndex,
				subscribers,
				next: null
			};
			this.publisherTail.next = node;
			this.publisherTail = this.publisherTail.next;
			this.publisherIndex += 1;
		}
		return true;
	}
	publishAll(elements) {
		if (this.publisherTail.subscribers !== 0) for (const a of elements) this.publish(a);
		else if (this.replayBuffer) this.replayBuffer.offerAll(elements);
		return [];
	}
	slide() {
		if (this.publisherHead !== this.publisherTail) {
			const node = this.publisherHead.next;
			const value = node.value;
			this.publisherHead = this.publisherHead.next;
			this.publisherHead.value = AbsentValue;
			this.subscribersIndex += 1;
			this.replayBuffer?.slide(value, node.replayIndex);
		}
	}
	subscribe() {
		this.publisherTail.subscribers += 1;
		return new UnboundedPubSubSubscription(this, this.publisherTail, this.publisherIndex, false);
	}
};
var UnboundedPubSubSubscription = class {
	self;
	subscriberHead;
	subscriberIndex;
	unsubscribed;
	constructor(self, subscriberHead, subscriberIndex, unsubscribed) {
		this.self = self;
		this.subscriberHead = subscriberHead;
		this.subscriberIndex = subscriberIndex;
		this.unsubscribed = unsubscribed;
	}
	isEmpty() {
		if (this.unsubscribed) return true;
		let empty = true;
		let loop = true;
		while (loop) if (this.subscriberHead === this.self.publisherTail) loop = false;
		else if (this.subscriberHead.next.value !== AbsentValue) {
			empty = false;
			loop = false;
		} else {
			this.subscriberHead = this.subscriberHead.next;
			this.subscriberIndex += 1;
		}
		return empty;
	}
	size() {
		if (this.unsubscribed) return 0;
		return this.self.publisherIndex - Math.max(this.subscriberIndex, this.self.subscribersIndex);
	}
	poll() {
		if (this.unsubscribed) return Empty;
		let loop = true;
		let polled = Empty;
		while (loop) if (this.subscriberHead === this.self.publisherTail) loop = false;
		else {
			const elem = this.subscriberHead.next.value;
			if (elem !== AbsentValue) {
				polled = elem;
				this.subscriberHead.subscribers -= 1;
				if (this.subscriberHead.subscribers === 0) {
					this.self.publisherHead = this.self.publisherHead.next;
					this.self.publisherHead.value = AbsentValue;
					this.self.subscribersIndex += 1;
				}
				loop = false;
			}
			this.subscriberHead = this.subscriberHead.next;
			this.subscriberIndex += 1;
		}
		return polled;
	}
	pollUpTo(n) {
		n = normalize$1(n);
		const builder = [];
		let i = 0;
		while (i !== n) {
			const a = this.poll();
			if (a === Empty) i = n;
			else {
				builder.push(a);
				i += 1;
			}
		}
		return builder;
	}
	unsubscribe() {
		if (!this.unsubscribed) {
			this.unsubscribed = true;
			this.self.publisherTail.subscribers -= 1;
			while (this.subscriberHead !== this.self.publisherTail) {
				if (this.subscriberHead.next.value !== AbsentValue) {
					this.subscriberHead.subscribers -= 1;
					if (this.subscriberHead.subscribers === 0) {
						this.self.publisherHead = this.self.publisherHead.next;
						this.self.publisherHead.value = AbsentValue;
						this.self.subscribersIndex += 1;
					}
				}
				this.subscriberHead = this.subscriberHead.next;
			}
		}
	}
};
var SubscriptionImpl = class {
	[SubscriptionTypeId] = { _A: identity$1 };
	pubsub;
	subscribers;
	subscription;
	pollers;
	shutdownHook;
	shutdownFlag;
	strategy;
	replayWindow;
	ended;
	constructor(pubsub, subscribers, subscription, pollers, shutdownHook, shutdownFlag, strategy, replayWindow, ended) {
		this.pubsub = pubsub;
		this.subscribers = subscribers;
		this.subscription = subscription;
		this.pollers = pollers;
		this.shutdownHook = shutdownHook;
		this.shutdownFlag = shutdownFlag;
		this.strategy = strategy;
		this.replayWindow = replayWindow;
		this.ended = ended;
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
};
var PubSubImpl = class {
	[TypeId$2] = { _A: identity$1 };
	pubsub;
	subscribers;
	scope;
	shutdownHook;
	shutdownFlag;
	strategy;
	ended;
	constructor(pubsub, subscribers, scope, shutdownHook, shutdownFlag, strategy, ended) {
		this.pubsub = pubsub;
		this.subscribers = subscribers;
		this.scope = scope;
		this.shutdownHook = shutdownHook;
		this.shutdownFlag = shutdownFlag;
		this.strategy = strategy;
		this.ended = ended;
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
};
const makePubSubUnsafe = (pubsub, subscribers, scope, shutdownHook, shutdownFlag, strategy, ended) => new PubSubImpl(pubsub, subscribers, scope, shutdownHook, shutdownFlag, strategy, ended);
/**
* Represents the dropping strategy for bounded `PubSub` values.
*
* **When to use**
*
* Use to keep publishers fast by dropping new messages when the `PubSub` is at
* capacity.
*
* **Details**
*
* A publish that arrives while the `PubSub` is full is dropped instead of
* waiting for capacity.
*
* **Gotchas**
*
* Subscribers may miss messages published while they are subscribed.
*
* **Example** (Applying a dropping strategy)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.scoped(Effect.gen(function*() {
*   // Explicitly create a PubSub with a dropping strategy
*   const pubsub = yield* PubSub.make<string>({
*     atomicPubSub: () => PubSub.makeAtomicBounded(2),
*     strategy: () => new PubSub.DroppingStrategy()
*   })
*
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Fill the PubSub
*   const pub1 = yield* PubSub.publish(pubsub, "msg1") // true
*   const pub2 = yield* PubSub.publish(pubsub, "msg2") // true
*   const pub3 = yield* PubSub.publish(pubsub, "msg3") // false (dropped)
*
*   // Subscribers will only see the first two messages
*   const messages = yield* PubSub.takeAll(subscription)
*   return { published: [pub1, pub2, pub3], messages }
* }))
*
* const actual = await Effect.runPromise(program)
* actual // => { published: [true, true, false], messages: ["msg1", "msg2"] }
* ```
*
* @stability stable
* @category models
* @since 4.0.0
*/
var DroppingStrategy = class {
	get shutdown() {
		return void_;
	}
	handleSurplus(_pubsub, _subscribers, _elements, _isShutdown) {
		return succeed$1(false);
	}
	onPubSubEmptySpaceUnsafe(_pubsub, _subscribers) {}
	completePollersUnsafe(pubsub, subscribers, subscription, pollers) {
		return strategyCompletePollersUnsafe(this, pubsub, subscribers, subscription, pollers);
	}
	completeSubscribersUnsafe(pubsub, subscribers) {
		return strategyCompleteSubscribersUnsafe(this, pubsub, subscribers);
	}
};
const strategyCompletePollersUnsafe = (strategy, pubsub, subscribers, subscription, pollers) => {
	let keepPolling = true;
	while (keepPolling && !subscription.isEmpty()) {
		const poller = take(pollers);
		if (poller === Empty) {
			removeSubscribers(subscribers, subscription, pollers);
			if (pollers.length === 0) keepPolling = false;
			else addSubscribers(subscribers, subscription, pollers);
		} else {
			const pollResult = subscription.poll();
			if (pollResult === Empty) prepend(pollers, poller);
			else {
				doneUnsafe(poller, succeed$2(pollResult));
				strategy.onPubSubEmptySpaceUnsafe(pubsub, subscribers);
			}
		}
	}
};
const strategyCompleteSubscribersUnsafe = (strategy, pubsub, subscribers) => {
	for (const [subscription, pollersSet] of subscribers) for (const pollers of pollersSet) strategy.completePollersUnsafe(pubsub, subscribers, subscription, pollers);
};
var ReplayBuffer = class {
	capacity;
	head = {
		value: AbsentValue,
		index: 0,
		next: null
	};
	tail = this.head;
	slideValues = [];
	size = 0;
	index = 0;
	publisherIndex = 0;
	constructor(capacity) {
		this.capacity = capacity;
	}
	slide(value, publisherIndex) {
		this.slideValues[this.index % this.capacity] = {
			value,
			index: publisherIndex
		};
		this.index++;
	}
	offer(a) {
		const index = this.publisherIndex++;
		this.tail.value = a;
		this.tail.index = index;
		this.tail.next = {
			value: AbsentValue,
			index: 0,
			next: null
		};
		this.tail = this.tail.next;
		if (this.size === this.capacity) {
			const dropped = this.head;
			this.head = dropped.next;
			dropped.next = null;
			dropped.value = AbsentValue;
		} else this.size += 1;
		return index;
	}
	offerAll(as) {
		for (const a of as) this.offer(a);
	}
};
var ReplayWindowImpl = class {
	buffer;
	values;
	index = 0;
	remaining;
	slideIndex;
	newestIndex = -1;
	constructor(buffer) {
		this.buffer = buffer;
		this.remaining = buffer.size;
		this.slideIndex = buffer.index;
		this.values = new Array(this.remaining);
		let node = buffer.head;
		for (let i = 0; i < this.remaining; i++) {
			this.values[i] = node.value;
			this.newestIndex = node.index;
			node = node.next;
		}
	}
	close() {
		this.values.length = 0;
		this.remaining = 0;
	}
	sync() {
		const slides = this.buffer.index - this.slideIndex;
		if (slides === 0 || this.remaining === 0) return;
		const count = Math.min(slides, this.buffer.capacity);
		const start = this.buffer.index - count;
		for (let i = 0; i < count; i++) {
			const entry = this.buffer.slideValues[(start + i) % this.buffer.capacity];
			if (entry.index > this.newestIndex) {
				this.index = (this.index + 1) % this.values.length;
				this.values[(this.index + this.remaining - 1) % this.values.length] = entry.value;
				this.newestIndex = entry.index;
			}
		}
		this.slideIndex = this.buffer.index;
	}
	take() {
		if (this.remaining === 0) return;
		this.sync();
		const value = this.values[this.index];
		this.values[this.index] = AbsentValue;
		this.index = (this.index + 1) % this.values.length;
		this.remaining--;
		if (this.remaining === 0) this.close();
		return value;
	}
	takeN(n) {
		n = normalize$1(n);
		const len = Math.min(n, this.remaining);
		const items = new Array(len);
		for (let i = 0; i < len; i++) items[i] = this.take();
		return items;
	}
	takeAll() {
		return this.takeN(this.remaining);
	}
};
const emptyReplayWindow = {
	remaining: 0,
	take: () => void 0,
	takeN: () => [],
	takeAll: () => [],
	close: () => void 0
};
const ChannelProto = {
	["~effect/Channel"]: {
		_Env: identity$1,
		_InErr: identity$1,
		_InElem: identity$1,
		_OutErr: identity$1,
		_OutElem: identity$1
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
/**
* Creates a `Channel` from a transformation function that operates on upstream pulls.
*
* **Example** (Creating channels from transforms)
*
* ```ts import.meta.vitest
* import { Channel, Effect } from "effect"
*
* const channel = Channel.fromTransform((upstream, scope) =>
*   Effect.succeed(upstream)
* )
* await Effect.runPromise(Channel.runCollect(channel)) // => []
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromTransform = (transform) => {
	const self = Object.create(ChannelProto);
	self.transform = (upstream, scope) => catchCause(transform(upstream, scope), (cause) => succeed$1(failCause(cause)));
	return self;
};
/**
* Creates a `Channel` from an `Effect` that produces a `Pull`.
*
* **Example** (Creating channels from pulls)
*
* ```ts import.meta.vitest
* import { Cause, Channel, Effect } from "effect"
*
* const channel = Channel.fromPull(Effect.sync(() => {
*   let emitted = false
*   return Effect.suspend(() => {
*     if (emitted) return Cause.done()
*     emitted = true
*     return Effect.succeed(42)
*   })
* }))
* await Effect.runPromise(Channel.runCollect(channel)) // => [42]
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromPull = (effect) => fromTransform((_, __) => effect);
/**
* Converts a `Channel` back to its underlying transformation function.
*
* **Example** (Extracting channel transforms)
*
* ```ts import.meta.vitest
* import { Channel, Effect } from "effect"
*
* const channel = Channel.succeed(42)
* const transform = Channel.toTransform(channel)
* typeof transform // => "function"
* Effect.runSync(Channel.runCollect(channel)) // => [42]
* ```
*
* @stability stable
* @category destructors
* @since 4.0.0
*/
const toTransform = (channel) => channel.transform;
/**
* Creates a channel from a PubSub subscription that outputs arrays of values.
*
* **Details**
*
* This constructor creates a channel that reads from a PubSub subscription and outputs
* arrays of values in chunks. It's useful when you want to process multiple values at once
* for better performance.
*
* **Example** (Batching subscription values)
*
* ```ts import.meta.vitest
* import { Channel, Data, Effect, Option, PubSub } from "effect"
*
* class StreamError extends Data.TaggedError("StreamError")<{
*   readonly message: string
* }> {}
*
* const program = Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<number>(16)
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Create a channel that reads arrays of values
*   const channel = Channel.fromSubscriptionArray(subscription)
*
*   // Publish some values
*   yield* PubSub.publish(pubsub, 1)
*   yield* PubSub.publish(pubsub, 2)
*   yield* PubSub.publish(pubsub, 3)
*   yield* PubSub.publish(pubsub, 4)
*
*   // The channel will output arrays like [1, 2, 3] and [4]
*   return channel
* })
* const result = Effect.scoped(Effect.flatMap(program, Channel.runHead))
* await Effect.runPromise(result) // => Option.some([1, 2, 3, 4])
* ```
*
* **Example** (Processing subscription values in batches)
*
* ```ts import.meta.vitest
* import { Channel, Data, Effect, Option, PubSub } from "effect"
*
* class BatchProcessingError extends Data.TaggedError("BatchProcessingError")<{
*   readonly reason: string
* }> {}
*
* const batchProcessor = Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<string>(32)
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Create a channel that processes items in batches
*   const batchChannel = Channel.fromSubscriptionArray(subscription)
*
*   // Transform to process each batch
*   const processedChannel = Channel.map(batchChannel, (batch) =>
*     batch.map((item) => item.toUpperCase())
*   )
*
*   yield* PubSub.publishAll(pubsub, ["one", "two"])
*   return processedChannel
* })
* const batch = Effect.scoped(Effect.flatMap(batchProcessor, Channel.runHead))
* await Effect.runPromise(batch) // => Option.some(["ONE", "TWO"])
* ```
*
* **Example** (Aggregating subscription metrics)
*
* ```ts import.meta.vitest
* import { Channel, Effect, Option, PubSub } from "effect"
*
* const metricsAggregator = Effect.gen(function*() {
*   const metricsPubSub = yield* PubSub.bounded<
*     { timestamp: number; value: number }
*   >(100)
*   const subscription = yield* PubSub.subscribe(metricsPubSub)
*
*   // Create a channel that collects metrics in chunks
*   const metricsChannel = Channel.fromSubscriptionArray(subscription)
*
*   // Transform to calculate aggregate statistics
*   const aggregatedChannel = Channel.map(metricsChannel, (metrics) => {
*     const values = metrics.map((m) => m.value)
*     const sum = values.reduce((a, b) => a + b, 0)
*     const avg = sum / values.length
*     const min = Math.min(...values)
*     const max = Math.max(...values)
*
*     return {
*       count: values.length,
*       sum,
*       average: avg,
*       min,
*       max,
*       firstTimestamp: Math.min(...metrics.map((m) => m.timestamp)),
*       lastTimestamp: Math.max(...metrics.map((m) => m.timestamp))
*     }
*   })
*
*   yield* PubSub.publish(metricsPubSub, { timestamp: 1, value: 10 })
*   return aggregatedChannel
* })
* const metric = Effect.scoped(Effect.flatMap(metricsAggregator, Channel.runHead))
* const result = await Effect.runPromise(metric)
* Option.map(result, ({ count, sum, average, min, max }) => ({ count, sum, average, min, max })) // => Option.some({ count: 1, sum: 10, average: 10, min: 10, max: 10 })
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromSubscriptionArray = (subscription) => fromPull(succeed$1(onInterrupt(takeAll(subscription), () => done$1())));
/**
* Creates a channel from a PubSub that outputs arrays of values.
*
* **Details**
*
* This constructor creates a channel that reads from a PubSub by automatically
* subscribing to it and collecting values into arrays. The channel outputs
* arrays of values in chunks, making it ideal for batch processing scenarios.
*
* **Example** (Batching PubSub values)
*
* ```ts import.meta.vitest
* import { Channel, Data, Effect, Option, PubSub } from "effect"
*
* class BatchError extends Data.TaggedError("BatchError")<{
*   readonly message: string
* }> {}
*
* const program = Effect.gen(function*() {
*   const pubsub = yield* PubSub.unbounded<number>({ replay: 4 })
*
*   // Create a channel that reads arrays of values
*   const channel = Channel.fromPubSubArray(pubsub)
*
*   // Publish some values
*   yield* PubSub.publish(pubsub, 1)
*   yield* PubSub.publish(pubsub, 2)
*   yield* PubSub.publish(pubsub, 3)
*   yield* PubSub.publish(pubsub, 4)
*
*   // The channel will output arrays like [1, 2, 3] and [4]
*   return channel
* })
* const result = Effect.scoped(Effect.flatMap(program, Channel.runHead))
* await Effect.runPromise(result) // => Option.some([1, 2, 3, 4])
* ```
*
* **Example** (Processing PubSub orders in batches)
*
* ```ts import.meta.vitest
* import { Channel, Effect, Option, PubSub } from "effect"
*
* interface Order {
*   readonly id: string
*   readonly customerId: string
*   readonly items: ReadonlyArray<string>
*   readonly total: number
*   readonly submittedAt: number
* }
*
* const orderBatchProcessor = Effect.gen(function*() {
*   const orderPubSub = yield* PubSub.unbounded<Order>({ replay: 1 })
*
*   // Create a channel that processes orders in batches
*   const orderChannel = Channel.fromPubSubArray(orderPubSub)
*
*   // Transform to process each batch of orders
*   const processedChannel = Channel.map(orderChannel, (orderBatch) => {
*     const totalRevenue = orderBatch.reduce((sum, order) => sum + order.total, 0)
*     const customerCount = new Set(orderBatch.map((order) =>
*       order.customerId
*     )).size
*
*     return {
*       batchSize: orderBatch.length,
*       totalRevenue,
*       uniqueCustomers: customerCount,
*       firstSubmittedAt: Math.min(...orderBatch.map((order) => order.submittedAt)),
*       orders: orderBatch
*     }
*   })
*
*   yield* PubSub.publish(orderPubSub, {
*     id: "1", customerId: "a", items: ["book"], total: 10, submittedAt: 1
*   })
*   return processedChannel
* })
* const order = Effect.scoped(Effect.flatMap(orderBatchProcessor, Channel.runHead))
* const result = await Effect.runPromise(order)
* Option.map(result, (batch) => [batch.batchSize, batch.totalRevenue, batch.uniqueCustomers]) // => Option.some([1, 10, 1])
* ```
*
* **Example** (Processing PubSub logs in batches)
*
* ```ts import.meta.vitest
* import { Channel, Effect, Option, PubSub } from "effect"
*
* interface LogEntry {
*   readonly timestamp: number
*   readonly level: "info" | "warn" | "error"
*   readonly message: string
*   readonly source: string
* }
*
* const logAggregator = Effect.gen(function*() {
*   const logPubSub = yield* PubSub.unbounded<LogEntry>({ replay: 1 })
*
*   // Create a channel that collects logs in batches
*   const logChannel = Channel.fromPubSubArray(logPubSub)
*
*   // Transform to analyze log batches
*   const analysisChannel = Channel.map(logChannel, (logBatch) => {
*     const errorCount = logBatch.filter((log) => log.level === "error").length
*     const warnCount = logBatch.filter((log) => log.level === "warn").length
*     const infoCount = logBatch.filter((log) => log.level === "info").length
*
*     const timeRange = {
*       start: Math.min(...logBatch.map((log) => log.timestamp)),
*       end: Math.max(...logBatch.map((log) => log.timestamp))
*     }
*
*     return {
*       batchId: `${timeRange.start}-${timeRange.end}`,
*       totalEntries: logBatch.length,
*       errorCount,
*       warnCount,
*       infoCount,
*       timeRange,
*       sources: [...new Set(logBatch.map((log) => log.source))]
*     }
*   })
*
*   yield* PubSub.publish(logPubSub, {
*     timestamp: 1,
*     level: "info",
*     message: "ready",
*     source: "app"
*   } satisfies LogEntry)
*   return analysisChannel
* })
* const log = Effect.scoped(Effect.flatMap(logAggregator, Channel.runHead))
* const result = await Effect.runPromise(log)
* Option.map(result, (batch) => [batch.batchId, batch.totalEntries, batch.infoCount]) // => Option.some(["1-1", 1, 1])
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromPubSubArray = (pubsub) => unwrap(map(subscribe(pubsub), fromSubscriptionArray));
/**
* Constructs a `Channel` from a scoped effect that will result in a
* `Channel` if successful.
*
* **Example** (Unwrapping channel effects)
*
* ```ts import.meta.vitest
* import { Channel, Data, Effect } from "effect"
*
* class UnwrapError extends Data.TaggedError("UnwrapError")<{
*   readonly reason: string
* }> {}
*
* // Create an effect that produces a channel
* const channelEffect = Effect.succeed(
*   Channel.fromIterable([1, 2, 3])
* )
*
* // Unwrap the effect to get the channel
* const unwrappedChannel = Channel.unwrap(channelEffect)
*
* Effect.runSync(Channel.runCollect(unwrappedChannel)) // => [1, 2, 3]
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const unwrap = (channel) => fromTransform((upstream, scope) => {
	let pull;
	return succeed$1(suspend(() => {
		if (pull) return pull;
		return channel.pipe(provide(scope), flatMap((channel) => toTransform(channel)(upstream, scope)), flatMap((pull_) => pull = pull_));
	}));
});
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/internal/stream.js
const TypeId = "~effect/Stream";
const streamVariance = {
	_R: identity$1,
	_E: identity$1,
	_A: identity$1
};
const Stream = function(channel) {
	this.channel = channel;
};
Stream.prototype = {
	[TypeId]: streamVariance,
	pipe() {
		return pipeArguments(this, arguments);
	}
};
/** @internal */
const fromChannel$1 = (channel) => new Stream(channel);
// node_modules/.pnpm/effect@4.1.2/node_modules/effect/dist/Stream.js
/**
* Creates a stream from a array-emitting `Channel`.
*
* **Example** (Creating a stream from an array-emitting channel)
*
* ```ts import.meta.vitest
* import { Channel, Effect, Stream } from "effect"
*
* const channel = Channel.succeed([1, 2, 3] as const)
* const stream = Stream.fromChannel(channel)
* await Effect.runPromise(Stream.runCollect(stream)) // => [1, 2, 3]
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fromChannel = fromChannel$1;
/**
* Creates a stream from a subscription to a `PubSub`.
*
* **Example** (Creating a stream from a subscription to a PubSub)
*
* ```ts import.meta.vitest
* import { Effect, Fiber, PubSub, Stream } from "effect"
*
* const program = Effect.gen(function*() {
*   const pubsub = yield* PubSub.unbounded<number>({ replay: 3 })
*
*   const fiber = yield* Stream.fromPubSub(pubsub).pipe(
*     Stream.take(3),
*     Stream.runCollect,
*     Effect.forkChild
*   )
*
*   yield* PubSub.publish(pubsub, 1)
*   yield* PubSub.publish(pubsub, 2)
*   yield* PubSub.publish(pubsub, 3)
*
*   const values = yield* Fiber.join(fiber)
*   values // => [1, 2, 3]
* })
*
* await Effect.runPromise(program)
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fromPubSub = (pubsub) => fromChannel(fromPubSubArray(pubsub));
// node_modules/.pnpm/pkce-challenge@5.0.1/node_modules/pkce-challenge/dist/index.node.js
let crypto;
crypto = globalThis.crypto?.webcrypto ?? globalThis.crypto ?? import("node:crypto").then((m) => m.webcrypto);
/**
* Creates an array of length `size` of random bytes
* @param size
* @returns Array of random ints (0 to 255)
*/
async function getRandomValues(size) {
	return (await crypto).getRandomValues(new Uint8Array(size));
}
/** Generate cryptographically strong random string
* @param size The desired length of the string
* @returns The random string
*/
async function random$1(size) {
	const mask = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~";
	const evenDistCutoff = Math.pow(2, 8) - Math.pow(2, 8) % 66;
	let result = "";
	while (result.length < size) {
		const randomBytes = await getRandomValues(size - result.length);
		for (const randomByte of randomBytes) if (randomByte < evenDistCutoff) result += mask[randomByte % 66];
	}
	return result;
}
/** Generate a PKCE challenge verifier
* @param length Length of the verifier
* @returns A random verifier `length` characters long
*/
async function generateVerifier(length) {
	return await random$1(length);
}
/** Generate a PKCE code challenge from a code verifier
* @param code_verifier
* @returns The base64 url encoded code challenge
*/
async function generateChallenge(code_verifier) {
	const buffer = await (await crypto).subtle.digest("SHA-256", new TextEncoder().encode(code_verifier));
	return btoa(String.fromCharCode(...new Uint8Array(buffer))).replace(/\//g, "_").replace(/\+/g, "-").replace(/=/g, "");
}
/** Generate a PKCE challenge pair
* @param length Length of the verifer (between 43-128). Defaults to 43.
* @returns PKCE challenge pair
*/
async function pkceChallenge(length) {
	if (!length) length = 43;
	if (length < 43 || length > 128) throw `Expected a length between 43 and 128. Received ${length}.`;
	const verifier = await generateVerifier(length);
	return {
		code_verifier: verifier,
		code_challenge: await generateChallenge(verifier)
	};
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/util.js
function getEnumValues(entries) {
	const numericValues = Object.values(entries).filter((v) => typeof v === "number");
	return Object.entries(entries).filter(([k, _]) => numericValues.indexOf(+k) === -1).map(([_, v]) => v);
}
function joinValues(array, separator = "|") {
	return array.map((val) => stringifyPrimitive(val)).join(separator);
}
function jsonStringifyReplacer(_, value) {
	if (typeof value === "bigint") return value.toString();
	return value;
}
var Cached = class {
	constructor(getter) {
		this._getter = getter;
		this._value = void 0;
	}
	get value() {
		const getter = this._getter;
		if (getter !== void 0) {
			this._value = getter();
			this._getter = void 0;
		}
		return this._value;
	}
};
function cached(getter) {
	return new Cached(getter);
}
function nullish(input) {
	return input === null || input === void 0;
}
function cleanRegex(source) {
	const start = source.startsWith("^") ? 1 : 0;
	const end = source.endsWith("$") ? source.length - 1 : source.length;
	return source.slice(start, end);
}
function floatSafeRemainder(val, step) {
	const ratio = val / step;
	const roundedRatio = Math.round(ratio);
	const tolerance = 4 * Number.EPSILON * Math.max(Math.abs(ratio), 1);
	if (Math.abs(ratio - roundedRatio) < tolerance) return 0;
	return ratio - roundedRatio;
}
function assignProp(target, prop, value) {
	Object.defineProperty(target, prop, {
		value,
		writable: true,
		enumerable: true,
		configurable: true
	});
}
/**
* Whichever object a def's `shape` currently answers from: the one the caller passed until the first read, the frozen copy after it.
*
* Its keys and descriptors read without invoking anything, which is what lets a discriminated union check its discriminator, and the cycle walk read a shape, without resolving a getter that references the schema being constructed. A def that answers `shape` from an accessor of its own has none.
*/
function rawShape(def) {
	const desc = Object.getOwnPropertyDescriptor(def, "shape");
	return desc?.get ? desc.get.raw : desc?.value;
}
function sourceShape(schema) {
	return rawShape(schema._zod.def) ?? schema._zod.def.shape;
}
function deferProp(target, key, getter) {
	Object.defineProperty(target, key, {
		get() {
			const value = getter();
			assignProp(this, key, value);
			return value;
		},
		enumerable: true,
		configurable: true
	});
}
function putProp(target, key, value) {
	if (key in target) assignProp(target, key, value);
	else target[key] = value;
}
/**
* Copies `keys` of `source`'s shape onto `target`, each value passed through `wrap`.
*
* A key the source has resolved is copied through now, so the derived shape states it outright and nothing has to resolve it to learn what it holds. A key the source still defers stays deferred, and reads back through the source's own `shape`, so it resolves once and both shapes get that one schema.
*/
function mirrorShape(target, source, keys, wrap) {
	const raw = sourceShape(source);
	for (const key of keys) {
		const desc = Object.getOwnPropertyDescriptor(raw, key);
		if (!desc.enumerable) continue;
		if (desc.get) deferProp(target, key, () => {
			const value = source._zod.def.shape[key];
			return wrap ? wrap(value, key) : value;
		});
		else putProp(target, key, wrap ? wrap(desc.value, key) : desc.value);
	}
}
function mirrorProps(target, source) {
	for (const key of Reflect.ownKeys(source)) {
		const desc = Object.getOwnPropertyDescriptor(source, key);
		if (!desc.enumerable) continue;
		if (desc.get) deferProp(target, key, () => source[key]);
		else putProp(target, key, desc.value);
	}
}
function mergeDefs(...defs) {
	const mergedDescriptors = {};
	for (const def of defs) {
		const descriptors = Object.getOwnPropertyDescriptors(def);
		Object.assign(mergedDescriptors, descriptors);
	}
	return Object.defineProperties({}, mergedDescriptors);
}
function esc(str) {
	return JSON.stringify(str);
}
function slugify(input) {
	return input.toLowerCase().trim().replace(/[^\w\s-]/g, "").replace(/[\s_-]+/g, "-").replace(/^-+|-+$/g, "");
}
const captureStackTrace = "captureStackTrace" in Error ? Error.captureStackTrace : (..._args) => {};
function isObject(data) {
	return typeof data === "object" && data !== null && !Array.isArray(data);
}
const allowsEval = /* @__PURE__*/ cached(() => {
	if (globalConfig.jitless) return false;
	if (typeof navigator !== "undefined" && navigator?.userAgent?.includes("Cloudflare")) return false;
	try {
		new Function("");
		return true;
	} catch (_) {
		return false;
	}
});
function isPlainObject(o) {
	if (isObject(o) === false) return false;
	const ctor = o.constructor;
	if (ctor === void 0) return true;
	if (typeof ctor !== "function") return true;
	const prot = ctor.prototype;
	if (isObject(prot) === false) return false;
	if (Object.prototype.hasOwnProperty.call(prot, "isPrototypeOf") === false) return false;
	return true;
}
function shallowClone(o) {
	if (isPlainObject(o)) return { ...o };
	if (Array.isArray(o)) return [...o];
	if (o instanceof Map) return new Map(o);
	if (o instanceof Set) return new Set(o);
	return o;
}
const propertyKeyTypes = /* @__PURE__*/ new Set([
	"string",
	"number",
	"symbol"
]);
function escapeRegex(str) {
	return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
function clone(inst, def, params) {
	const cl = new inst._zod.constr(def ?? inst._zod.def);
	if (!def || params?.parent) cl._zod.parent = inst;
	return cl;
}
function normalizeParams(_params) {
	const params = _params;
	if (!params) return {};
	if (typeof params === "string") return { error: () => params };
	if (params?.message !== void 0) {
		if (params?.error !== void 0) throw new Error("Cannot specify both `message` and `error` params");
		params.error = params.message;
	}
	delete params.message;
	if (typeof params.error === "string") return {
		...params,
		error: () => params.error
	};
	return params;
}
function stringifyPrimitive(value) {
	if (typeof value === "bigint") return value.toString() + "n";
	if (typeof value === "string") return `"${value}"`;
	return `${value}`;
}
function optionalKeys(shape) {
	return Object.keys(shape).filter((k) => {
		return shape[k]._zod.optin !== void 0 && shape[k]._zod.optout === "optional";
	});
}
const NUMBER_FORMAT_RANGES = /*@__PURE__*/ (() => ({
	safeint: [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER],
	int32: [-2147483648, 2147483647],
	uint32: [0, 4294967295],
	float32: [-34028234663852886e22, 34028234663852886e22],
	float64: [-Number.MAX_VALUE, Number.MAX_VALUE]
}))();
const BIGINT_FORMAT_RANGES = {
	int64: [/* @__PURE__*/ BigInt("-9223372036854775808"), /* @__PURE__*/ BigInt("9223372036854775807")],
	uint64: [/* @__PURE__*/ BigInt(0), /* @__PURE__*/ BigInt("18446744073709551615")]
};
function pick(schema, mask) {
	const currDef = schema._zod.def;
	const checks = currDef.checks;
	if (checks && checks.length > 0) throw new Error(".pick() cannot be used on object schemas containing refinements");
	const newShape = {};
	mirrorShape(newShape, schema, maskedKeys(schema, mask));
	return clone(schema, mergeDefs(currDef, {
		shape: newShape,
		checks: []
	}));
}
function maskedKeys(schema, mask) {
	const raw = sourceShape(schema);
	const keys = [];
	for (const key of Reflect.ownKeys(mask)) {
		if (!Object.getOwnPropertyDescriptor(raw, key)?.enumerable) throw new Error(`Unrecognized key: "${String(key)}"`);
		if (mask[key]) keys.push(key);
	}
	return keys;
}
function omit(schema, mask) {
	const currDef = schema._zod.def;
	const checks = currDef.checks;
	if (checks && checks.length > 0) throw new Error(".omit() cannot be used on object schemas containing refinements");
	const omitted = new Set(maskedKeys(schema, mask));
	const newShape = {};
	mirrorShape(newShape, schema, Reflect.ownKeys(sourceShape(schema)).filter((key) => !omitted.has(key)));
	return clone(schema, mergeDefs(currDef, {
		shape: newShape,
		checks: []
	}));
}
function extend(schema, shape) {
	if (!isPlainObject(shape)) throw new Error("Invalid input to extend: expected a plain object");
	const checks = schema._zod.def.checks;
	if (checks && checks.length > 0) {
		const existingShape = sourceShape(schema);
		for (const key of Reflect.ownKeys(shape)) if (Object.getOwnPropertyDescriptor(existingShape, key) !== void 0) throw new Error("Cannot overwrite keys on object schemas containing refinements. Use `.safeExtend()` instead.");
	}
	return clone(schema, mergeDefs(schema._zod.def, { shape: extended(schema, shape) }));
}
function extended(schema, shape) {
	const newShape = {};
	mirrorShape(newShape, schema, Reflect.ownKeys(sourceShape(schema)));
	mirrorProps(newShape, shape);
	return newShape;
}
function safeExtend(schema, shape) {
	if (!isPlainObject(shape)) throw new Error("Invalid input to safeExtend: expected a plain object");
	return clone(schema, mergeDefs(schema._zod.def, { shape: extended(schema, shape) }));
}
function merge(a, b) {
	if (!b?._zod?.def) throw new Error("Invalid input to merge: expected an object schema. To merge a plain shape, use `.extend()`.");
	if (a._zod.def.checks?.length) throw new Error(".merge() cannot be used on object schemas containing refinements. Use .safeExtend() instead.");
	const newShape = {};
	mirrorShape(newShape, a, Reflect.ownKeys(sourceShape(a)));
	mirrorShape(newShape, b, Reflect.ownKeys(sourceShape(b)));
	return clone(a, mergeDefs(a._zod.def, {
		shape: newShape,
		get catchall() {
			return b._zod.def.catchall;
		},
		checks: b._zod.def.checks ?? []
	}));
}
function partial(Class, schema, mask, name = "partial") {
	const checks = schema._zod.def.checks;
	if (checks && checks.length > 0) throw new Error(`.${name}() cannot be used on object schemas containing refinements`);
	const selected = mask ? new Set(maskedKeys(schema, mask)) : void 0;
	const newShape = {};
	mirrorShape(newShape, schema, Reflect.ownKeys(sourceShape(schema)), Class && ((value, key) => selected && !selected.has(key) ? value : new Class({
		type: "optional",
		innerType: value
	})));
	return clone(schema, mergeDefs(schema._zod.def, {
		shape: newShape,
		checks: []
	}));
}
function required(Class, schema, mask) {
	const selected = mask ? new Set(maskedKeys(schema, mask)) : void 0;
	const newShape = {};
	mirrorShape(newShape, schema, Reflect.ownKeys(sourceShape(schema)), (value, key) => selected && !selected.has(key) ? value : new Class({
		type: "nonoptional",
		innerType: value
	}));
	return clone(schema, mergeDefs(schema._zod.def, { shape: newShape }));
}
function aborted(x, startIndex = 0) {
	if (x.aborted === true) return true;
	for (let i = startIndex; i < x.issues.length; i++) if (x.issues[i]?.continue !== true) return true;
	return false;
}
function explicitlyAborted(x, startIndex = 0) {
	if (x.aborted === true) return true;
	for (let i = startIndex; i < x.issues.length; i++) if (x.issues[i]?.continue === false) return true;
	return false;
}
function prefixIssues(path, issues) {
	return issues.map((iss) => {
		var _a;
		(_a = iss).path ?? (_a.path = []);
		iss.path.unshift(path);
		return iss;
	});
}
function unwrapMessage(message) {
	return typeof message === "string" ? message : message?.message;
}
/** The leading size checks of `defChecks` that set an upper bound, each paired with it. */
function sizeCaps(defChecks) {
	const caps = [];
	for (const ch of defChecks ?? []) {
		const d = ch._zod.def;
		if (!/_(length|size)$|_equals$/.test(d.check)) break;
		const max = d.maximum ?? d.length ?? d.size;
		if (max >= 0) caps.push([max, ch]);
	}
	return caps;
}
function attachSchema(issues, start, inst) {
	var _a;
	for (let i = start; i < issues.length; i++) (_a = issues[i]).schema ?? (_a.schema = inst);
}
function finalizeIssue(iss, ctx, config) {
	var _a;
	const traits = iss.inst?._zod?.traits;
	if (traits?.has("$ZodType")) {
		if (traits.has("$ZodCheck")) (_a = iss).schema ?? (_a.schema = iss.inst);
		else iss.schema = iss.inst;
	}
	const schemaError = iss.schema !== iss.inst ? iss.schema?._zod.def?.error : void 0;
	const message = iss.message ? iss.message : unwrapMessage(iss.inst?._zod.def?.error?.(iss)) ?? unwrapMessage(schemaError?.(iss)) ?? unwrapMessage(ctx?.error?.(iss)) ?? unwrapMessage(config.customError?.(iss)) ?? unwrapMessage(config.localeError?.(iss)) ?? "Invalid input";
	const full = {};
	for (const k of Object.keys(iss)) {
		if (k === "inst" || k === "schema" || k === "continue" || k === "input" || k === "__proto__") continue;
		full[k] = iss[k];
	}
	full.path ?? (full.path = []);
	full.message = message;
	if (ctx?.reportInput) full.input = iss.input;
	return full;
}
const highSurrogate = /[\uD800-\uDBFF]/;
function codePointLength(str) {
	const units = str.length;
	if (!highSurrogate.test(str)) return units;
	let count = units;
	for (let i = 0; i < units - 1; i++) if ((str.charCodeAt(i) & 64512) === 55296 && (str.charCodeAt(i + 1) & 64512) === 56320) {
		count--;
		i++;
	}
	return count;
}
function getLengthableOrigin(input) {
	if (Array.isArray(input)) return "array";
	if (typeof input === "string") return "string";
	return "unknown";
}
function parsedType(data) {
	const t = typeof data;
	switch (t) {
		case "number": return Number.isNaN(data) ? "nan" : "number";
		case "object": {
			if (data === null) return "null";
			if (Array.isArray(data)) return "array";
			const obj = data;
			if (obj && Object.getPrototypeOf(obj) !== Object.prototype && "constructor" in obj && obj.constructor) return obj.constructor.name;
		}
	}
	return t;
}
function issue(...args) {
	const [iss, input, inst] = args;
	if (typeof iss === "string") return {
		message: iss,
		code: "custom",
		input,
		inst
	};
	return { ...iss };
}
/**
* Installs a trait's members on its prototype. Each value builds that member for the instance on first read; the built value shadows the accessor as an own property, so a detached `const { parse } = schema` keeps working.
*
* Call this from a `proto` initializer, which runs once per prototype — never per instance.
*/
function members(proto, table) {
	for (const key in table) {
		const desc = Object.getOwnPropertyDescriptor(table, key);
		if (desc.get) Object.defineProperty(proto, key, {
			...desc,
			enumerable: false
		});
		else defineBound(proto, key, desc.value);
	}
}
/** Shadows a prototype member with an own value, so a getter that builds from the instance runs once. */
function own(inst, key, value, enumerable = true) {
	Object.defineProperty(inst, key, {
		configurable: true,
		writable: true,
		enumerable,
		value
	});
	return value;
}
/** Like {@link own}, for a member that was never an own data property and has to stay out of `Object.keys`. */
function hide(inst, key, value) {
	return own(inst, key, value, false);
}
/** Adds members a table derives from the instance: each builds on first read and shadows as own data, and assignment shadows the same way, as when these were own properties. */
/* @__NO_SIDE_EFFECTS__ */
function derived(computes, table) {
	for (const key in computes) {
		const compute = computes[key];
		Object.defineProperty(table, key, {
			configurable: true,
			enumerable: true,
			get() {
				return own(this, key, compute(this));
			},
			set(value) {
				own(this, key, value);
			}
		});
	}
	return table;
}
function defineBound(proto, key, fn) {
	Object.defineProperty(proto, key, {
		configurable: true,
		get() {
			return this == null ? fn : own(this, key, fn.bind(this));
		},
		set(value) {
			own(this, key, value);
		}
	});
}
/** Returns the prototype to install on, or `undefined` if this group is already installed on it. */
function claim(inst, sentinel) {
	const proto = Object.getPrototypeOf(inst);
	return sentinel in proto ? void 0 : proto;
}
let installing;
let broke = false;
const breaker = {
	configurable: true,
	get() {
		broke = true;
	}
};
/**
* Installs a lazily-derived internal on the `_zod` prototype of `inst`'s
* constructor, computed from the internals object itself and cached there on
* first read. One accessor per constructor rather than one per instance.
*/
function defineLazyInternal(inst, key, compute) {
	const proto = Object.getPrototypeOf(inst._zod);
	if (key in proto && installing !== inst._zod) {
		installing = void 0;
		return;
	}
	installing = inst._zod;
	Object.defineProperty(proto, key, {
		configurable: true,
		get() {
			Object.defineProperty(this, key, breaker);
			const outer = broke;
			broke = false;
			try {
				const value = compute(this);
				if (broke) delete this[key];
				else Object.defineProperty(this, key, {
					configurable: true,
					writable: true,
					value
				});
				broke = broke || outer;
				return value;
			} catch (err) {
				delete this[key];
				broke = broke || outer;
				throw err;
			}
		},
		set(value) {
			Object.defineProperty(this, key, {
				configurable: true,
				writable: true,
				value
			});
		}
	});
}
/**
* Installs `key` on `inst`'s prototype, computed by `make` on first read and cached there as an own
* data property. One accessor per constructor rather than one per instance, because an own accessor
* puts every instance after the first into v8 dictionary mode. The key doubles as the sentinel.
*/
function installLazyProp(inst, key, make, enumerable) {
	const proto = claim(inst, key);
	if (!proto) return;
	Object.defineProperty(proto, key, {
		configurable: true,
		get() {
			const desc = {
				configurable: true,
				writable: true,
				enumerable,
				value: void 0
			};
			Object.defineProperty(this, key, desc);
			desc.value = make(this);
			Object.defineProperty(this, key, desc);
			return desc.value;
		},
		set(value) {
			Object.defineProperty(this, key, {
				configurable: true,
				writable: true,
				enumerable,
				value
			});
		}
	});
}
/** Marks the thunk `_catch` synthesises for a constant catch value. `Function.length` cannot tell that thunk from a user callback — rest and defaulted parameters both report arity 0 — and a user callback reads `ctx.error`, whose issues only finalize correctly against the caller's per-parse error map. Provenance can say what arity cannot. A plain string key rather than `Symbol.for`, whose call at module scope no bundler can prove pure — the same shape that anchored `urlCanParse` into every build. */
const CONSTANT_CATCH = "~constantCatch";
/** Wraps a constant catch value in a thunk tagged with {@link CONSTANT_CATCH}. */
function constantCatch(value) {
	const fn = () => value;
	fn[CONSTANT_CATCH] = true;
	return fn;
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/core.js
var _a$1;
/** A special constant with type `never` */
const NEVER = /*@__PURE__*/ Object.freeze({ status: "aborted" });
const _zodDesc = {
	value: void 0,
	enumerable: false
};
let _E = "captureStackTrace" in Error ? Error : null;
function newError(Definition) {
	const E = _E;
	if (E) {
		const saved = E.stackTraceLimit;
		if (typeof saved === "number") {
			try {
				E.stackTraceLimit = 0;
			} catch {
				_E = null;
				return new Definition();
			}
			try {
				return new Definition();
			} finally {
				E.stackTraceLimit = saved;
			}
		}
	}
	return new Definition();
}
/* @__NO_SIDE_EFFECTS__ */
function $constructor(name, initializer, proto, params) {
	const zodProto = {};
	function Internals(def) {
		this.def = def;
		this.constr = _;
		this.traits = /* @__PURE__ */ new Set();
	}
	Internals.prototype = zodProto;
	const protoMembers = proto;
	const initialized = protoMembers && /* @__PURE__ */ new WeakSet();
	function init(inst, def) {
		if (!inst._zod) {
			_zodDesc.value = new Internals(def);
			try {
				Object.defineProperty(inst, "_zod", _zodDesc);
			} finally {
				_zodDesc.value = void 0;
			}
		} else if (inst._zod.traits.has(name)) return;
		inst._zod.traits.add(name);
		initializer(inst, def);
		if (initialized) {
			const own = Object.getPrototypeOf(inst);
			const ctorProto = inst._zod.constr.prototype;
			let up = own;
			while (up && up !== ctorProto) up = Object.getPrototypeOf(up);
			const target = up ?? own;
			if (!initialized.has(target)) {
				initialized.add(target);
				members(target, protoMembers);
			}
		}
		const proto = _.prototype;
		for (const k in proto) {
			if (!Object.prototype.hasOwnProperty.call(proto, k)) continue;
			if (!(k in inst)) inst[k] = proto[k].bind(inst);
		}
	}
	const Parent = params?.Parent ?? Object;
	class Definition extends Parent {}
	Object.defineProperty(Definition, "name", { value: name });
	function _(def) {
		const inst = params?.Parent ? newError(Definition) : this;
		init(inst, def);
		const deferred = inst._zod.deferred;
		if (deferred) {
			for (const fn of deferred) fn();
			inst._zod.deferred = void 0;
		}
		const pp = globalThis.__zod_globalConfig?.postProcessor;
		if (pp) pp(inst);
		return inst;
	}
	Object.defineProperty(_, "init", { value: init });
	Object.defineProperty(_, Symbol.hasInstance, { value: (inst) => {
		if (params?.Parent && inst instanceof params.Parent) return true;
		return inst?._zod?.traits?.has(name);
	} });
	Object.defineProperty(_, "name", { value: name });
	return _;
}
var $ZodAsyncError = class extends Error {
	constructor() {
		super(`Encountered Promise during synchronous parse. Use .parseAsync() instead.`);
	}
};
var $ZodEncodeError = class extends Error {
	constructor(name) {
		super(`Encountered unidirectional transform during encode: ${name}`);
		this.name = "ZodEncodeError";
	}
};
(_a$1 = globalThis).__zod_globalConfig ?? (_a$1.__zod_globalConfig = {});
const globalConfig = globalThis.__zod_globalConfig;
function config(newConfig) {
	if (newConfig) Object.assign(globalConfig, newConfig);
	return globalConfig;
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/errors.js
function _getMessage() {
	const internals = this._zod;
	internals.message ?? (internals.message = JSON.stringify(internals.def, jsonStringifyReplacer, 2));
	return internals.message;
}
function _setMessage(value) {
	this._zod.message = value;
}
const _messageDesc = {
	get: _getMessage,
	set: _setMessage,
	enumerable: true,
	configurable: true
};
const _issuesDesc = {
	value: void 0,
	enumerable: false
};
const _installedToString = /* @__PURE__ */ new WeakSet([Object.prototype, Error.prototype]);
const initializer$1 = (inst, def) => {
	inst.name = "$ZodError";
	_issuesDesc.value = def;
	Object.defineProperty(inst, "issues", _issuesDesc);
	_issuesDesc.value = void 0;
	Object.defineProperty(inst, "message", _messageDesc);
	const proto = Object.getPrototypeOf(inst);
	if (!_installedToString.has(proto)) {
		_installedToString.add(proto);
		Object.defineProperty(proto, "toString", {
			configurable: true,
			enumerable: false,
			get() {
				const value = () => this.message;
				Object.defineProperty(this, "toString", {
					value,
					configurable: true,
					writable: true
				});
				return value;
			},
			set(value) {
				Object.defineProperty(this, "toString", {
					value,
					configurable: true,
					writable: true
				});
			}
		});
	}
};
const $ZodError = /* @__PURE__ */ $constructor("$ZodError", initializer$1);
/** Get-or-create `obj[key]` as an own data property. A path segment naming an inherited member
* ("toString", "constructor") would otherwise read through to the prototype, and assigning
* "__proto__" would hit the setter instead of creating a key. */
function node(obj, key, make) {
	if (!Object.prototype.hasOwnProperty.call(obj, key)) {
		if (key === "__proto__") Object.defineProperty(obj, key, {
			value: make(),
			writable: true,
			enumerable: true,
			configurable: true
		});
		else obj[key] = make();
	}
	return obj[key];
}
function flattenError(error, mapper = (issue) => issue.message) {
	const fieldErrors = {};
	const formErrors = [];
	for (const sub of error.issues) if (sub.path.length > 0) node(fieldErrors, sub.path[0], () => []).push(mapper(sub));
	else formErrors.push(mapper(sub));
	return {
		formErrors,
		fieldErrors
	};
}
function formatError(error, mapper = (issue) => issue.message) {
	const fieldErrors = { _errors: [] };
	const processError = (error, path = []) => {
		for (const issue of error.issues) if (issue.code === "invalid_union" && issue.errors.length) issue.errors.map((issues) => processError({ issues }, [...path, ...issue.path]));
		else if (issue.code === "invalid_key") processError({ issues: issue.issues }, [...path, ...issue.path]);
		else if (issue.code === "invalid_element") processError({ issues: issue.issues }, [...path, ...issue.path]);
		else {
			const fullpath = [...path, ...issue.path];
			if (fullpath.length === 0) fieldErrors._errors.push(mapper(issue));
			else {
				let curr = fieldErrors;
				let i = 0;
				while (i < fullpath.length) {
					const el = fullpath[i];
					const terminal = i === fullpath.length - 1;
					if (el === "_errors") {
						if (terminal) curr._errors.push(mapper(issue));
						i++;
						continue;
					}
					if (!Object.prototype.hasOwnProperty.call(curr, el)) Object.defineProperty(curr, el, {
						value: { _errors: [] },
						enumerable: true,
						writable: true,
						configurable: true
					});
					const node = curr[el];
					if (terminal) node._errors.push(mapper(issue));
					curr = node;
					i++;
				}
			}
		}
	};
	processError(error);
	return fieldErrors;
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/parse.js
function finalizeParams(callee, params) {
	return {
		callee: params?.callee ?? callee,
		Err: params?.Err
	};
}
const _parse = (_Err) => {
	const fn = (schema, value, _ctx, _params) => {
		const ctx = _ctx ? {
			..._ctx,
			async: false
		} : { async: false };
		const result = schema._zod.run({
			value,
			issues: []
		}, ctx);
		if (result instanceof Promise) throw new $ZodAsyncError();
		if (result.issues.length) {
			const e = new ((_params?.Err) ?? _Err)(result.issues.map((iss) => finalizeIssue(iss, ctx, config())));
			captureStackTrace(e, _params?.callee ?? fn);
			throw e;
		}
		return result.value;
	};
	return fn;
};
const _parseAsync = (_Err) => {
	const fn = async (schema, value, _ctx, params) => {
		const ctx = _ctx ? {
			..._ctx,
			async: true
		} : { async: true };
		let result = schema._zod.run({
			value,
			issues: []
		}, ctx);
		if (result instanceof Promise) result = await result;
		if (result.issues.length) {
			const e = new ((params?.Err) ?? _Err)(result.issues.map((iss) => finalizeIssue(iss, ctx, config())));
			captureStackTrace(e, params?.callee ?? fn);
			throw e;
		}
		return result.value;
	};
	return fn;
};
const _safeParse = (_Err) => (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		async: false
	} : { async: false };
	const result = schema._zod.run({
		value,
		issues: []
	}, ctx);
	if (result instanceof Promise) throw new $ZodAsyncError();
	return result.issues.length ? failure(_Err, result.issues, ctx) : {
		success: true,
		data: result.value
	};
};
function failure(Err, issues, ctx) {
	let error;
	return {
		success: false,
		get error() {
			if (!error) {
				error = new Err(issues.map((iss) => finalizeIssue(iss, ctx, config())));
				issues = void 0;
				ctx = void 0;
			}
			return error;
		},
		set error(e) {
			error = e;
			issues = void 0;
			ctx = void 0;
		}
	};
}
const _safeParseAsync = (_Err) => async (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		async: true
	} : { async: true };
	let result = schema._zod.run({
		value,
		issues: []
	}, ctx);
	if (result instanceof Promise) result = await result;
	return result.issues.length ? failure(_Err, result.issues, ctx) : {
		success: true,
		data: result.value
	};
};
const COMPILE_INVALID = /* @__PURE__ */ Symbol.for("zod.compile.invalid");
const COMPILE_FALLBACK = /* @__PURE__ */ Symbol.for("zod.compile.fallback");
const validate = ((schema, value, _ctx) => {
	const validator = schema._zod.bag.validator;
	if (validator !== void 0) {
		if (validator(value) !== COMPILE_INVALID) return true;
		if (validator.definite === true && _ctx === void 0) return false;
	}
	return validateFallback(schema, value, _ctx);
});
function validateFallback(schema, value, _ctx) {
	const ctx = _ctx ? {
		..._ctx,
		async: false,
		abortEarly: true
	} : {
		async: false,
		abortEarly: true
	};
	const fallbackRun = schema._zod.bag.fallbackRun;
	let result;
	if (fallbackRun) {
		ctx[COMPILE_FALLBACK] = true;
		result = fallbackRun({
			value,
			issues: []
		}, ctx);
	} else result = schema._zod.run({
		value,
		issues: []
	}, ctx);
	if (result instanceof Promise) throw new $ZodAsyncError();
	return result.issues.length === 0;
}
const validateAsync$1 = async (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		async: true,
		abortEarly: true
	} : {
		async: true,
		abortEarly: true
	};
	let result = schema._zod.run({
		value,
		issues: []
	}, ctx);
	if (result instanceof Promise) result = await result;
	return result.issues.length === 0;
};
const _encode = (_Err) => {
	const parse = _parse(_Err);
	const fn = (schema, value, _ctx, _params) => {
		const ctx = _ctx ? {
			..._ctx,
			direction: "backward"
		} : { direction: "backward" };
		return parse(schema, value, ctx, finalizeParams(fn, _params));
	};
	return fn;
};
const _decode = (_Err) => {
	const parse = _parse(_Err);
	const fn = (schema, value, _ctx, _params) => {
		return parse(schema, value, _ctx, finalizeParams(fn, _params));
	};
	return fn;
};
const _encodeAsync = (_Err) => {
	const parseAsync = _parseAsync(_Err);
	const fn = async (schema, value, _ctx, _params) => {
		const ctx = _ctx ? {
			..._ctx,
			direction: "backward"
		} : { direction: "backward" };
		return await parseAsync(schema, value, ctx, finalizeParams(fn, _params));
	};
	return fn;
};
const _decodeAsync = (_Err) => {
	const parseAsync = _parseAsync(_Err);
	const fn = async (schema, value, _ctx, _params) => {
		return await parseAsync(schema, value, _ctx, finalizeParams(fn, _params));
	};
	return fn;
};
const _safeEncode = (_Err) => (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		direction: "backward"
	} : { direction: "backward" };
	return _safeParse(_Err)(schema, value, ctx);
};
const _safeDecode = (_Err) => (schema, value, _ctx) => {
	return _safeParse(_Err)(schema, value, _ctx);
};
const _safeEncodeAsync = (_Err) => async (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		direction: "backward"
	} : { direction: "backward" };
	return _safeParseAsync(_Err)(schema, value, ctx);
};
const _safeDecodeAsync = (_Err) => async (schema, value, _ctx) => {
	return _safeParseAsync(_Err)(schema, value, _ctx);
};
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/regexes.js
/**
* @deprecated CUID v1 is deprecated by its authors due to information leakage
* (timestamps embedded in the id). Use {@link cuid2} instead.
* See https://github.com/paralleldrive/cuid.
*/
const cuid = /^[cC][0-9a-z]{6,}$/;
const cuid2 = /^[0-9a-z]+$/;
const ulid = /^[0-7][0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{25}$/;
const xid = /^[0-9a-vA-V]{20}$/;
const ksuid = /^[A-Za-z0-9]{27}$/;
const nanoid = /^[a-zA-Z0-9_-]{21}$/;
function nanoidOfLength(length) {
	return new RegExp(`^[a-zA-Z0-9_-]{${length}}$`);
}
/** ISO 8601-1 duration regex. Does not support the 8601-2 extensions like negative durations or fractional/negative components. */
const duration = /^P(?:(\d+W)|(?!.*W)(?=\d|T\d)(\d+Y)?(\d+M)?(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+([.,]\d+)?S)?)?)$/;
/** A regex for any UUID-like identifier: 8-4-4-4-12 hex pattern */
const guid = /^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$/;
/** Returns a regex for validating an RFC 9562/4122 UUID.
*
* @param version Optionally specify a version 1-8. If no version is specified, all versions are supported. */
const uuid = (version) => {
	if (!version) return /^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$/;
	return new RegExp(`^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-${version}[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12})$`);
};
/** Practical email validation */
const email = /^(?:[A-Za-z0-9_'+\-]+\.)*[A-Za-z0-9_'+\-]*[A-Za-z0-9_+-]@(?:[A-Za-z0-9][A-Za-z0-9\-]*\.)+[A-Za-z]{2,}$/;
const _emoji$1 = `^(?=[\\s\\S]*[\\p{Extended_Pictographic}\\p{Regional_Indicator}\\u20E3])[\\p{Extended_Pictographic}\\p{Emoji_Component}]+$`;
function emoji() {
	return new RegExp(_emoji$1, "u");
}
const ipv4 = /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$/;
const ipv6 = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:))$/;
const cidrv4 = /^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\/([0-9]|[1-2][0-9]|3[0-2])$/;
const cidrv6 = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:))\/(12[0-8]|1[01][0-9]|[1-9]?[0-9])$/;
const base64 = /^$|^(?:[0-9a-zA-Z+/]{4})*(?:(?:[0-9a-zA-Z+/]{2}==)|(?:[0-9a-zA-Z+/]{3}=))?$/;
const base64url = /^(?:[A-Za-z0-9_-]{4})*(?:[A-Za-z0-9_-]{2,3})?$/;
const httpProtocol = /^https?$/;
const e164 = /^\+[1-9]\d{6,14}$/;
const dateSource = `(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))`;
/** Anchors a pattern source. The interpolation lives here rather than at the call site because
* esbuild will not drop a `@__PURE__` call whose own argument interpolates a variable, but it
* will drop `anchor(dateSource)`. Keeping it inline pinned `date` into every bundle. */
function anchor(source) {
	return new RegExp(`^${source}$`);
}
const date = /*@__PURE__*/ anchor(dateSource);
function timeSource(args) {
	const hhmm = `(?:[01]\\d|2[0-3]):[0-5]\\d`;
	return typeof args.precision === "number" ? args.precision === -1 ? `${hhmm}` : args.precision === 0 ? `${hhmm}:[0-5]\\d` : `${hhmm}:[0-5]\\d\\.\\d{${args.precision}}` : args.seconds ? `${hhmm}:[0-5]\\d(?:\\.\\d+)?` : `${hhmm}(?::[0-5]\\d(?:\\.\\d+)?)?`;
}
function time(args) {
	return new RegExp(`^${timeSource(args)}$`);
}
function datetime$1(args) {
	const opts = ["Z"];
	if (args.offset) opts.push(`([+-](?:[01]\\d|2[0-3]):[0-5]\\d)`);
	const qualified = `${timeSource({
		precision: args.precision,
		seconds: true
	})}(?:${opts.join("|")})`;
	const timeRegex = args.local ? `${qualified}|${timeSource({ precision: args.precision })}` : qualified;
	return new RegExp(`^${dateSource}T(?:${timeRegex})$`);
}
const anyString = /^[\s\S]{0,}$/;
const integer = /^-?\d+$/;
const number$2 = /^-?\d+(?:\.\d+)?$/;
const boolean$1 = /^(?:true|false)$/i;
const _null$2 = /^null$/i;
const lowercase = /^[^A-Z]*$/;
const uppercase = /^[^a-z]*$/;
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/checks.js
const $ZodCheck = /*@__PURE__*/ $constructor("$ZodCheck", (inst, def) => {
	var _a;
	inst._zod ?? (inst._zod = {});
	inst._zod.def = def;
	(_a = inst._zod).onattach ?? (_a.onattach = []);
});
/** Default `when` for length-based checks: run only on non-nullish values with a `length`. */
const _whenHasLength = (payload) => {
	const val = payload.value;
	return !nullish(val) && val.length !== void 0;
};
const numericOriginMap = {
	number: "number",
	bigint: "bigint",
	object: "date"
};
const $ZodCheckLessThan = /*@__PURE__*/ $constructor("$ZodCheckLessThan", (inst, def) => {
	$ZodCheck.init(inst, def);
	const origin = numericOriginMap[typeof def.value];
	inst._zod.check = (payload) => {
		if (def.inclusive ? payload.value <= def.value : payload.value < def.value) return;
		payload.issues.push({
			origin: numericOriginMap[typeof payload.value] ?? origin,
			code: "too_big",
			maximum: typeof def.value === "object" ? def.value.getTime() : def.value,
			input: payload.value,
			inclusive: def.inclusive,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckGreaterThan = /*@__PURE__*/ $constructor("$ZodCheckGreaterThan", (inst, def) => {
	$ZodCheck.init(inst, def);
	const origin = numericOriginMap[typeof def.value];
	inst._zod.check = (payload) => {
		if (def.inclusive ? payload.value >= def.value : payload.value > def.value) return;
		payload.issues.push({
			origin: numericOriginMap[typeof payload.value] ?? origin,
			code: "too_small",
			minimum: typeof def.value === "object" ? def.value.getTime() : def.value,
			input: payload.value,
			inclusive: def.inclusive,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckMultipleOf = /*@__PURE__*/ $constructor("$ZodCheckMultipleOf", (inst, def) => {
	$ZodCheck.init(inst, def);
	inst._zod.check = (payload) => {
		if (typeof payload.value !== typeof def.value) throw new Error("Cannot mix number and bigint in multiple_of check.");
		if (typeof payload.value === "bigint" ? def.value !== BigInt(0) && payload.value % def.value === BigInt(0) : floatSafeRemainder(payload.value, def.value) === 0) return;
		payload.issues.push({
			origin: typeof payload.value,
			code: "not_multiple_of",
			divisor: def.value,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckNumberFormat = /*@__PURE__*/ $constructor("$ZodCheckNumberFormat", (inst, def) => {
	$ZodCheck.init(inst, def);
	def.format = def.format || "float64";
	const isInt = def.format?.includes("int");
	const origin = isInt ? "int" : "number";
	const [minimum, maximum] = NUMBER_FORMAT_RANGES[def.format];
	inst._zod.check = (payload) => {
		const input = payload.value;
		if (isInt) {
			if (!Number.isInteger(input)) {
				payload.issues.push({
					expected: origin,
					format: def.format,
					code: "invalid_type",
					continue: false,
					input,
					inst
				});
				return;
			}
			if (!Number.isSafeInteger(input)) {
				if (input > 0) payload.issues.push({
					input,
					code: "too_big",
					maximum: Number.MAX_SAFE_INTEGER,
					note: "Integers must be within the safe integer range.",
					inst,
					origin,
					inclusive: true,
					continue: !def.abort
				});
				else payload.issues.push({
					input,
					code: "too_small",
					minimum: Number.MIN_SAFE_INTEGER,
					note: "Integers must be within the safe integer range.",
					inst,
					origin,
					inclusive: true,
					continue: !def.abort
				});
				return;
			}
		}
		if (input < minimum) payload.issues.push({
			origin: "number",
			input,
			code: "too_small",
			minimum,
			inclusive: true,
			inst,
			continue: !def.abort
		});
		if (input > maximum) payload.issues.push({
			origin: "number",
			input,
			code: "too_big",
			maximum,
			inclusive: true,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckMaxLength = /*@__PURE__*/ $constructor("$ZodCheckMaxLength", (inst, def) => {
	var _a;
	$ZodCheck.init(inst, def);
	(_a = inst._zod.def).when ?? (_a.when = _whenHasLength);
	inst._zod.check = (payload) => {
		const input = payload.value;
		const units = input.length;
		if ((typeof input === "string" && units > def.maximum ? codePointLength(input) : units) <= def.maximum) return;
		const origin = getLengthableOrigin(input);
		payload.issues.push({
			origin,
			code: "too_big",
			maximum: def.maximum,
			inclusive: true,
			input,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckMinLength = /*@__PURE__*/ $constructor("$ZodCheckMinLength", (inst, def) => {
	var _a;
	$ZodCheck.init(inst, def);
	(_a = inst._zod.def).when ?? (_a.when = _whenHasLength);
	inst._zod.check = (payload) => {
		const input = payload.value;
		const units = input.length;
		if ((typeof input === "string" && units >= def.minimum && units < def.minimum * 2 ? codePointLength(input) : units) >= def.minimum) return;
		const origin = getLengthableOrigin(input);
		payload.issues.push({
			origin,
			code: "too_small",
			minimum: def.minimum,
			inclusive: true,
			input,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckLengthEquals = /*@__PURE__*/ $constructor("$ZodCheckLengthEquals", (inst, def) => {
	var _a;
	$ZodCheck.init(inst, def);
	(_a = inst._zod.def).when ?? (_a.when = _whenHasLength);
	inst._zod.check = (payload) => {
		const input = payload.value;
		const units = input.length;
		const length = typeof input === "string" && units >= def.length && units <= def.length * 2 ? codePointLength(input) : units;
		if (length === def.length) return;
		const origin = getLengthableOrigin(input);
		const tooBig = length > def.length;
		payload.issues.push({
			origin,
			...tooBig ? {
				code: "too_big",
				maximum: def.length
			} : {
				code: "too_small",
				minimum: def.length
			},
			inclusive: true,
			exact: true,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckStringFormat = /*@__PURE__*/ $constructor("$ZodCheckStringFormat", (inst, def) => {
	var _a, _b;
	$ZodCheck.init(inst, def);
	if (def.pattern) (_a = inst._zod).check ?? (_a.check = (payload) => {
		def.pattern.lastIndex = 0;
		if (def.pattern.test(payload.value)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: def.format,
			input: payload.value,
			...def.pattern ? { pattern: def.pattern.toString() } : {},
			inst,
			continue: !def.abort
		});
	});
	else (_b = inst._zod).check ?? (_b.check = () => {});
});
const $ZodCheckRegex = /*@__PURE__*/ $constructor("$ZodCheckRegex", (inst, def) => {
	$ZodCheckStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		def.pattern.lastIndex = 0;
		if (def.pattern.test(payload.value)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: "regex",
			input: payload.value,
			pattern: def.pattern.toString(),
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckLowerCase = /*@__PURE__*/ $constructor("$ZodCheckLowerCase", (inst, def) => {
	def.pattern ?? (def.pattern = lowercase);
	$ZodCheckStringFormat.init(inst, def);
});
const $ZodCheckUpperCase = /*@__PURE__*/ $constructor("$ZodCheckUpperCase", (inst, def) => {
	def.pattern ?? (def.pattern = uppercase);
	$ZodCheckStringFormat.init(inst, def);
});
const $ZodCheckIncludes = /*@__PURE__*/ $constructor("$ZodCheckIncludes", (inst, def) => {
	$ZodCheck.init(inst, def);
	const escapedRegex = escapeRegex(def.includes);
	def.pattern = new RegExp(typeof def.position === "number" ? `^.{${def.position},}${escapedRegex}` : escapedRegex);
	inst._zod.check = (payload) => {
		if (payload.value.includes(def.includes, def.position)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: "includes",
			includes: def.includes,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckStartsWith = /*@__PURE__*/ $constructor("$ZodCheckStartsWith", (inst, def) => {
	$ZodCheck.init(inst, def);
	const pattern = new RegExp(`^${escapeRegex(def.prefix)}.*`);
	def.pattern ?? (def.pattern = pattern);
	inst._zod.check = (payload) => {
		if (payload.value.startsWith(def.prefix)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: "starts_with",
			prefix: def.prefix,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckEndsWith = /*@__PURE__*/ $constructor("$ZodCheckEndsWith", (inst, def) => {
	$ZodCheck.init(inst, def);
	const pattern = new RegExp(`.*${escapeRegex(def.suffix)}$`);
	def.pattern ?? (def.pattern = pattern);
	inst._zod.check = (payload) => {
		if (payload.value.endsWith(def.suffix)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: "ends_with",
			suffix: def.suffix,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckOverwrite = /*@__PURE__*/ $constructor("$ZodCheckOverwrite", (inst, def) => {
	$ZodCheck.init(inst, def);
	inst._zod.check = (payload) => {
		payload.value = def.tx(payload.value);
	};
});
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/doc.js
var Doc = class {
	constructor(args = [], closed = {}) {
		this.content = [];
		this.indent = 0;
		this.args = args;
		this.closed = closed;
	}
	indented(fn) {
		this.indent += 1;
		try {
			fn(this);
		} finally {
			this.indent -= 1;
		}
	}
	write(arg) {
		if (typeof arg === "function") {
			arg(this, { execution: "sync" });
			arg(this, { execution: "async" });
			return;
		}
		const lines = arg.split("\n").filter((x) => x);
		const minIndent = Math.min(...lines.map((x) => x.length - x.trimStart().length));
		const dedented = lines.map((x) => x.slice(minIndent)).map((x) => " ".repeat(this.indent * 2) + x);
		for (const line of dedented) this.content.push(line);
	}
	compile() {
		const F = Function;
		const content = this?.content ?? [``];
		return new F(...Object.keys(this.closed), `return function (${this.args.join(", ")}) {\n${content.join("\n")}\n};`)(...Object.values(this.closed));
	}
};
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/versions.js
const version = {
	major: 4,
	minor: 6,
	patch: 7
};
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/schemas.js
const $ZodType = /*@__PURE__*/ $constructor("$ZodType", (inst, def) => {
	var _a;
	inst ?? (inst = {});
	inst._zod.def = def;
	inst._zod.bag = inst._zod.bag || {};
	inst._zod.version = version;
	const defChecks = inst._zod.def.checks;
	const checks = inst._zod.traits.has("$ZodCheck") ? [inst, ...defChecks ?? []] : defChecks?.length ? [...defChecks] : [];
	for (const ch of checks) for (const fn of ch._zod.onattach) fn(inst);
	if (checks.length === 0) {
		(_a = inst._zod).deferred ?? (_a.deferred = []);
		inst._zod.deferred?.push(() => {
			inst._zod.run = inst._zod.parse;
		});
	} else {
		const runChecks = (payload, checks, ctx) => {
			if (payload.memo) return payload;
			let isAborted = aborted(payload);
			let asyncResult;
			for (const ch of checks) {
				if (ch._zod.def.when) {
					if (explicitlyAborted(payload)) continue;
					if (!ch._zod.def.when(payload)) continue;
				} else if (isAborted) continue;
				const currLen = payload.issues.length;
				const _ = ch._zod.check(payload);
				if (_ instanceof Promise && ctx?.async === false) throw new $ZodAsyncError();
				if (asyncResult || _ instanceof Promise) asyncResult = (asyncResult ?? Promise.resolve()).then(async () => {
					await _;
					if (payload.issues.length === currLen) return;
					attachSchema(payload.issues, currLen, inst);
					if (!isAborted) isAborted = aborted(payload, currLen);
				});
				else {
					if (payload.issues.length === currLen) continue;
					attachSchema(payload.issues, currLen, inst);
					if (!isAborted) isAborted = aborted(payload, currLen);
				}
			}
			if (asyncResult) return asyncResult.then(() => {
				return payload;
			});
			return payload;
		};
		const handleCanaryResult = (canary, payload, ctx) => {
			if (aborted(canary)) {
				canary.aborted = true;
				return canary;
			}
			const checkResult = runChecks(payload, checks, ctx);
			if (checkResult instanceof Promise) {
				if (ctx.async === false) throw new $ZodAsyncError();
				return checkResult.then((checkResult) => inst._zod.parse(checkResult, ctx));
			}
			return inst._zod.parse(checkResult, ctx);
		};
		inst._zod.run = (payload, ctx) => {
			if (ctx.skipChecks) return inst._zod.parse(payload, ctx);
			if (ctx.direction === "backward") {
				const canary = inst._zod.parse({
					value: payload.value,
					issues: []
				}, {
					...ctx,
					skipChecks: true
				});
				if (canary instanceof Promise) return canary.then((canary) => {
					return handleCanaryResult(canary, payload, ctx);
				});
				return handleCanaryResult(canary, payload, ctx);
			}
			const result = inst._zod.parse(payload, ctx);
			if (result instanceof Promise) {
				if (ctx.async === false) throw new $ZodAsyncError();
				return result.then((result) => runChecks(result, checks, ctx));
			}
			return runChecks(result, checks, ctx);
		};
	}
}, {
	get "~standard"() {
		return hide(this, "~standard", standardProps(this));
	},
	set "~standard"(value) {
		own(this, "~standard", value);
	}
});
/** The Standard Schema surface for `inst`. Shared so wrappers can extend it without forcing it. */
const toStandardResult = (r, ctx) => r.issues.length ? { issues: r.issues.map((iss) => finalizeIssue(iss, ctx, config())) } : { value: r.value };
async function validateAsync(inst, value) {
	const ctx = { async: true };
	return toStandardResult(await inst._zod.run({
		value,
		issues: []
	}, ctx), ctx);
}
function standardProps(inst) {
	return {
		validate: (value) => {
			const ctx = { async: false };
			try {
				const r = inst._zod.run({
					value,
					issues: []
				}, ctx);
				if (!(r instanceof Promise)) return toStandardResult(r, ctx);
			} catch (_) {}
			return validateAsync(inst, value);
		},
		vendor: "zod",
		version: 1
	};
}
const $ZodString = /*@__PURE__*/ $constructor("$ZodString", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.pattern = def.pattern ?? anyString;
	inst._zod.parse = (payload, _) => {
		if (def.coerce) try {
			payload.value = String(payload.value);
		} catch (_) {}
		if (typeof payload.value === "string") return payload;
		payload.issues.push({
			expected: "string",
			code: "invalid_type",
			input: payload.value,
			inst
		});
		return payload;
	};
});
const $ZodStringFormat = /*@__PURE__*/ $constructor("$ZodStringFormat", (inst, def) => {
	$ZodCheckStringFormat.init(inst, def);
	$ZodString.init(inst, def);
});
const $ZodGUID = /*@__PURE__*/ $constructor("$ZodGUID", (inst, def) => {
	def.pattern ?? (def.pattern = guid);
	$ZodStringFormat.init(inst, def);
});
const $ZodUUID = /*@__PURE__*/ $constructor("$ZodUUID", (inst, def) => {
	if (def.version) {
		const v = {
			v1: 1,
			v2: 2,
			v3: 3,
			v4: 4,
			v5: 5,
			v6: 6,
			v7: 7,
			v8: 8
		}[def.version];
		if (v === void 0) throw new Error(`Invalid UUID version: "${def.version}"`);
		def.pattern ?? (def.pattern = uuid(v));
	} else def.pattern ?? (def.pattern = uuid());
	$ZodStringFormat.init(inst, def);
});
const $ZodEmail = /*@__PURE__*/ $constructor("$ZodEmail", (inst, def) => {
	def.pattern ?? (def.pattern = email);
	$ZodStringFormat.init(inst, def);
});
function canParseURL(input) {
	try {
		if (typeof URL !== "undefined" && typeof URL.canParse === "function") return URL.canParse(input);
		new URL(input);
		return true;
	} catch {
		return false;
	}
}
function validateURL(trimmed, def) {
	if (!("normalize" in def) && !("hostname" in def) && !("protocol" in def)) return canParseURL(trimmed) || 2;
	return parseURLObject(trimmed, def);
}
/** Parses a URL while preserving the non-normalizing HTTP guard. */
function parseURLObject(trimmed, def) {
	if (!def.normalize && def.protocol?.source === httpProtocol.source && !/^https?:\/\//i.test(trimmed)) return 1;
	try {
		if (typeof URL !== "undefined") {
			const URLStatic = URL;
			if (typeof URLStatic.parse === "function") return URLStatic.parse(trimmed) ?? 2;
		}
		return new URL(trimmed);
	} catch {
		return 2;
	}
}
const asciiTabOrNewline = /[\t\n\r]/g;
/** The URL parser deletes every ASCII tab, LF and CR from its input before it parses, so `new URL("https://exa\nmple.com")` reports on `example.com`. Applying the same deletion to the returned value closes the half of that divergence which can move the host; the parser's other rewrite, stripping C0 controls at the edges, cannot. */
function stripTabAndNewline(value) {
	return value.replace(asciiTabOrNewline, "");
}
function urlHostnameOk(url, hostname) {
	hostname.lastIndex = 0;
	return hostname.test(url.hostname);
}
function urlProtocolOk(url, protocol) {
	protocol.lastIndex = 0;
	return protocol.test(url.protocol.endsWith(":") ? url.protocol.slice(0, -1) : url.protocol);
}
const $ZodURL = /*@__PURE__*/ $constructor("$ZodURL", (inst, def) => {
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		try {
			const trimmed = payload.value.trim();
			const url = validateURL(trimmed, def);
			if (url === 1) {
				payload.issues.push({
					code: "invalid_format",
					format: "url",
					note: "Invalid URL format",
					input: payload.value,
					inst,
					continue: !def.abort
				});
				return;
			}
			if (url === 2) {
				payload.issues.push({
					code: "invalid_format",
					format: "url",
					input: payload.value,
					inst,
					continue: !def.abort
				});
				return;
			}
			if (url === true) {
				payload.value = stripTabAndNewline(trimmed);
				return;
			}
			if (def.hostname && !urlHostnameOk(url, def.hostname)) payload.issues.push({
				code: "invalid_format",
				format: "url",
				note: "Invalid hostname",
				pattern: def.hostname.source,
				input: payload.value,
				inst,
				continue: !def.abort
			});
			if (def.protocol && !urlProtocolOk(url, def.protocol)) payload.issues.push({
				code: "invalid_format",
				format: "url",
				note: "Invalid protocol",
				pattern: def.protocol.source,
				input: payload.value,
				inst,
				continue: !def.abort
			});
			payload.value = def.normalize ? url.href : stripTabAndNewline(trimmed);
			return;
		} catch (_) {
			payload.issues.push({
				code: "invalid_format",
				format: "url",
				input: payload.value,
				inst,
				continue: !def.abort
			});
		}
	};
});
const $ZodEmoji = /*@__PURE__*/ $constructor("$ZodEmoji", (inst, def) => {
	def.pattern ?? (def.pattern = emoji());
	$ZodStringFormat.init(inst, def);
});
const $ZodNanoID = /*@__PURE__*/ $constructor("$ZodNanoID", (inst, def) => {
	if (def.length !== void 0 && (!Number.isInteger(def.length) || def.length < 1)) throw new Error(`Invalid nanoid length: ${def.length}`);
	def.pattern ?? (def.pattern = def.length === void 0 ? nanoid : nanoidOfLength(def.length));
	$ZodStringFormat.init(inst, def);
});
/**
* @deprecated CUID v1 is deprecated by its authors due to information leakage
* (timestamps embedded in the id). Use {@link $ZodCUID2} instead.
* See https://github.com/paralleldrive/cuid.
*/
const $ZodCUID = /*@__PURE__*/ $constructor("$ZodCUID", (inst, def) => {
	def.pattern ?? (def.pattern = cuid);
	$ZodStringFormat.init(inst, def);
});
const $ZodCUID2 = /*@__PURE__*/ $constructor("$ZodCUID2", (inst, def) => {
	def.pattern ?? (def.pattern = cuid2);
	$ZodStringFormat.init(inst, def);
});
const $ZodULID = /*@__PURE__*/ $constructor("$ZodULID", (inst, def) => {
	def.pattern ?? (def.pattern = ulid);
	$ZodStringFormat.init(inst, def);
});
const $ZodXID = /*@__PURE__*/ $constructor("$ZodXID", (inst, def) => {
	def.pattern ?? (def.pattern = xid);
	$ZodStringFormat.init(inst, def);
});
const $ZodKSUID = /*@__PURE__*/ $constructor("$ZodKSUID", (inst, def) => {
	def.pattern ?? (def.pattern = ksuid);
	$ZodStringFormat.init(inst, def);
});
const $ZodISODateTime = /*@__PURE__*/ $constructor("$ZodISODateTime", (inst, def) => {
	def.pattern ?? (def.pattern = datetime$1(def));
	$ZodStringFormat.init(inst, def);
});
const $ZodISODate = /*@__PURE__*/ $constructor("$ZodISODate", (inst, def) => {
	def.pattern ?? (def.pattern = date);
	$ZodStringFormat.init(inst, def);
});
const $ZodISOTime = /*@__PURE__*/ $constructor("$ZodISOTime", (inst, def) => {
	def.pattern ?? (def.pattern = time(def));
	$ZodStringFormat.init(inst, def);
});
const $ZodISODuration = /*@__PURE__*/ $constructor("$ZodISODuration", (inst, def) => {
	def.pattern ?? (def.pattern = duration);
	$ZodStringFormat.init(inst, def);
});
const $ZodIPv4 = /*@__PURE__*/ $constructor("$ZodIPv4", (inst, def) => {
	def.pattern ?? (def.pattern = ipv4);
	$ZodStringFormat.init(inst, def);
});
/** An IPv6 address is written with hex digits, colons and dots, and nothing else. The guard is what makes the check below an IPv6 check: `new URL("http://[...]")` parses an authority, not an address, so `@` and `\` re-delimit it and `"::@1\\"` validates against the host `0.0.0.1`. The URL parser also deletes ASCII tab, LF and CR rather than failing, which is how `"::1\n"` validated as `::1`. */
const ipv6Alphabet = /^[0-9a-fA-F:.]+$/;
function isValidIPv6(value) {
	if (!ipv6Alphabet.test(value)) return false;
	return canParseURL(`http://[${value}]`);
}
const $ZodIPv6 = /*@__PURE__*/ $constructor("$ZodIPv6", (inst, def) => {
	def.pattern ?? (def.pattern = ipv6);
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (!isValidIPv6(payload.value)) payload.issues.push({
			code: "invalid_format",
			format: "ipv6",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCIDRv4 = /*@__PURE__*/ $constructor("$ZodCIDRv4", (inst, def) => {
	def.pattern ?? (def.pattern = cidrv4);
	$ZodStringFormat.init(inst, def);
});
function isValidCIDRv6(value) {
	const parts = value.split("/");
	if (parts.length !== 2) return false;
	const [address, prefix] = parts;
	if (!prefix) return false;
	const prefixNum = Number(prefix);
	if (`${prefixNum}` !== prefix) return false;
	if (prefixNum < 0 || prefixNum > 128) return false;
	return isValidIPv6(address);
}
const $ZodCIDRv6 = /*@__PURE__*/ $constructor("$ZodCIDRv6", (inst, def) => {
	def.pattern ?? (def.pattern = cidrv6);
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (!isValidCIDRv6(payload.value)) payload.issues.push({
			code: "invalid_format",
			format: "cidrv6",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
function isValidBase64(data) {
	if (data === "") return true;
	if (/\s/.test(data)) return false;
	if (data.length % 4 !== 0) return false;
	try {
		atob(data);
		return true;
	} catch {
		return false;
	}
}
const base64Charset = /^[0-9a-zA-Z+/]*={0,2}$/;
const $ZodBase64 = /*@__PURE__*/ $constructor("$ZodBase64", (inst, def) => {
	def.pattern ?? (def.pattern = base64Charset);
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (isValidBase64(payload.value)) return;
		payload.issues.push({
			code: "invalid_format",
			format: "base64",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const base64urlCharset = /^[A-Za-z0-9_-]*$/;
function isValidBase64URL(data) {
	if (!base64urlCharset.test(data)) return false;
	const base64 = data.replace(/[-_]/g, (c) => c === "-" ? "+" : "/");
	return isValidBase64(base64.padEnd(Math.ceil(base64.length / 4) * 4, "="));
}
const $ZodBase64URL = /*@__PURE__*/ $constructor("$ZodBase64URL", (inst, def) => {
	def.pattern ?? (def.pattern = base64urlCharset);
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (isValidBase64URL(payload.value)) return;
		payload.issues.push({
			code: "invalid_format",
			format: "base64url",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodE164 = /*@__PURE__*/ $constructor("$ZodE164", (inst, def) => {
	def.pattern ?? (def.pattern = e164);
	$ZodStringFormat.init(inst, def);
});
function isValidJWT(token, algorithm = null) {
	try {
		const tokensParts = token.split(".");
		if (tokensParts.length !== 3) return false;
		const [header] = tokensParts;
		if (!header) return false;
		const parsedHeader = JSON.parse(atob(header));
		if ("typ" in parsedHeader && parsedHeader?.typ !== "JWT") return false;
		if (!parsedHeader.alg) return false;
		if (algorithm && (!("alg" in parsedHeader) || parsedHeader.alg !== algorithm)) return false;
		return true;
	} catch {
		return false;
	}
}
const $ZodJWT = /*@__PURE__*/ $constructor("$ZodJWT", (inst, def) => {
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (isValidJWT(payload.value, def.alg)) return;
		payload.issues.push({
			code: "invalid_format",
			format: "jwt",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodNumber = /*@__PURE__*/ $constructor("$ZodNumber", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.pattern = number$2;
	inst._zod.parse = (payload, _ctx) => {
		if (def.coerce) try {
			payload.value = Number(payload.value);
		} catch (_) {}
		const input = payload.value;
		if (typeof input === "number" && !Number.isNaN(input) && Number.isFinite(input)) return payload;
		const received = typeof input === "number" ? Number.isNaN(input) ? "NaN" : !Number.isFinite(input) ? String(input) : void 0 : void 0;
		payload.issues.push({
			expected: "number",
			code: "invalid_type",
			input,
			inst,
			...received ? { received } : {}
		});
		return payload;
	};
});
const $ZodNumberFormat = /*@__PURE__*/ $constructor("$ZodNumberFormat", (inst, def) => {
	$ZodCheckNumberFormat.init(inst, def);
	$ZodNumber.init(inst, def);
});
const $ZodBoolean = /*@__PURE__*/ $constructor("$ZodBoolean", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.pattern = boolean$1;
	inst._zod.parse = (payload, _ctx) => {
		if (def.coerce) try {
			payload.value = Boolean(payload.value);
		} catch (_) {}
		const input = payload.value;
		if (typeof input === "boolean") return payload;
		payload.issues.push({
			expected: "boolean",
			code: "invalid_type",
			input,
			inst
		});
		return payload;
	};
});
const $ZodNull = /*@__PURE__*/ $constructor("$ZodNull", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.pattern = _null$2;
	inst._zod.values = /* @__PURE__ */ new Set([null]);
	inst._zod.parse = (payload, _ctx) => {
		const input = payload.value;
		if (input === null) return payload;
		payload.issues.push({
			expected: "null",
			code: "invalid_type",
			input,
			inst
		});
		return payload;
	};
});
const $ZodAny = /*@__PURE__*/ $constructor("$ZodAny", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.parse = (payload) => payload;
});
const $ZodUnknown = /*@__PURE__*/ $constructor("$ZodUnknown", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.parse = (payload) => payload;
});
const $ZodNever = /*@__PURE__*/ $constructor("$ZodNever", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.parse = (payload, _ctx) => {
		payload.issues.push({
			expected: "never",
			code: "invalid_type",
			input: payload.value,
			inst
		});
		return payload;
	};
});
function capSize(inst, size) {
	const caps = sizeCaps(inst._zod.def.checks);
	if (!caps.length) return;
	const cap = Math.min(...caps.map((c) => c[0]));
	const parse = inst._zod.parse;
	inst._zod.parse = (payload, ctx) => {
		const n = size(payload.value);
		if (n > cap && !ctx?.skipChecks && ctx?.direction !== "backward") {
			for (const [max, ch] of sizeCaps(inst._zod.def.checks)) if (n > max && ch._zod.def.when(payload)) {
				const start = payload.issues.length;
				ch._zod.check(payload);
				payload.issues[start].continue = false;
				attachSchema(payload.issues, start, inst);
				return payload;
			}
		}
		return parse(payload, ctx);
	};
}
const arraySize = (input) => Array.isArray(input) ? input.length : -1;
function handleArrayResult(result, final, index) {
	if (result.issues.length) final.issues.push(...prefixIssues(index, result.issues));
	final.value[index] = result.value;
}
const $ZodArray = /*@__PURE__*/ $constructor("$ZodArray", (inst, def) => {
	$ZodType.init(inst, def);
	const memo = globalConfig.memoizer;
	memo?.attach(inst);
	inst._zod.parse = (payload, ctx) => {
		const input = payload.value;
		if (!Array.isArray(input)) {
			payload.issues.push({
				expected: "array",
				code: "invalid_type",
				input,
				inst
			});
			return payload;
		}
		payload.value = memo ? memo.alloc(inst, payload, Array(input.length), ctx) : Array(input.length);
		const proms = [];
		const abortEarly = ctx?.abortEarly;
		for (let i = 0; i < input.length; i++) {
			const item = input[i];
			const result = def.element._zod.run({
				value: item,
				issues: []
			}, ctx);
			if (result instanceof Promise) proms.push(result.then((result) => handleArrayResult(result, payload, i)));
			else {
				handleArrayResult(result, payload, i);
				if (abortEarly && result.issues.length !== 0 && aborted(result)) break;
			}
		}
		if (proms.length) return Promise.all(proms).then(() => payload);
		return payload;
	};
	capSize(inst, arraySize);
});
function handlePropertyResult(result, final, key, input, optin, optout) {
	const isPresent = key in input;
	const isOptionalOut = optout === "optional";
	if (!isPresent && isOptionalOut && optin === "optional") return;
	if (result.issues.length) {
		if (optin !== void 0 && isOptionalOut && !isPresent) return;
		final.issues.push(...prefixIssues(key, result.issues));
	}
	if (!isPresent && optin === void 0) {
		if (!result.issues.length) final.issues.push({
			code: "invalid_type",
			expected: "nonoptional",
			input: void 0,
			path: [key]
		});
		return;
	}
	if (result.value === void 0) {
		if (isPresent || optin === "defaulted" && !isOptionalOut) final.value[key] = void 0;
	} else final.value[key] = result.value;
}
const NO_SYMBOL_KEYS = [];
function normalizeDef(def) {
	const keys = Object.keys(def.shape);
	const ownSymbols = Object.getOwnPropertySymbols(def.shape);
	const symbolKeys = ownSymbols.length ? ownSymbols : NO_SYMBOL_KEYS;
	const allKeys = symbolKeys.length ? [...keys, ...symbolKeys] : keys;
	for (const k of allKeys) if (!def.shape?.[k]?._zod?.traits?.has("$ZodType")) throw new Error(`Invalid element at key "${String(k)}": expected a Zod schema`);
	const okeys = optionalKeys(def.shape);
	return {
		...def,
		allKeys,
		symbolKeys,
		keySet: new Set(keys),
		numKeys: keys.length,
		optionalKeys: new Set(okeys)
	};
}
function handleCatchall(proms, input, payload, ctx, def, inst, abortEarly) {
	const unrecognized = [];
	const keySet = def.keySet;
	const _catchall = def.catchall._zod;
	const t = _catchall.def.type;
	const optin = _catchall.optin;
	const optout = _catchall.optout;
	let seen = 0;
	for (const key in input) {
		if (abortEarly && payload.issues.length !== seen) {
			if (aborted(payload, seen)) break;
			seen = payload.issues.length;
		}
		if (keySet.has(key)) continue;
		if (key === "__proto__") {
			if (t === "never") unrecognized.push(key);
			continue;
		}
		if (t === "never") {
			unrecognized.push(key);
			continue;
		}
		const r = _catchall.run({
			value: input[key],
			issues: []
		}, ctx);
		if (r instanceof Promise) proms.push(r.then((r) => handlePropertyResult(r, payload, key, input, optin, optout)));
		else handlePropertyResult(r, payload, key, input, optin, optout);
	}
	if (unrecognized.length) payload.issues.push({
		code: "unrecognized_keys",
		keys: unrecognized,
		input,
		inst,
		continue: true
	});
	if (!proms.length) return payload;
	return Promise.all(proms).then(() => {
		return payload;
	});
}
const $ZodObject = /*@__PURE__*/ $constructor("$ZodObject", (inst, def) => {
	$ZodType.init(inst, def);
	const desc = Object.getOwnPropertyDescriptor(def, "shape");
	const sh = desc?.get ? desc.get.raw : def.shape ?? {};
	if (sh) {
		const get = () => {
			const newSh = { ...sh };
			Object.defineProperty(def, "shape", { value: newSh });
			get.raw = newSh;
			return newSh;
		};
		get.raw = sh;
		Object.defineProperty(def, "shape", { get });
	}
	const _normalized = cached(() => normalizeDef(def));
	defineLazyInternal(inst, "propValues", (zod) => {
		const shape = zod.def.shape;
		const propValues = {};
		for (const key in shape) {
			const field = shape[key]._zod;
			if (field.values) {
				if (!Object.prototype.hasOwnProperty.call(propValues, key)) assignProp(propValues, key, /* @__PURE__ */ new Set());
				for (const v of field.values) propValues[key].add(v);
				if (field.optin !== void 0) propValues[key].add(void 0);
			}
		}
		return propValues;
	});
	const isObject$2 = isObject;
	const catchall = def.catchall;
	let value;
	const memo = globalConfig.memoizer;
	memo?.attach(inst);
	inst._zod.parse = (payload, ctx) => {
		value ?? (value = _normalized.value);
		const input = payload.value;
		if (!isObject$2(input)) {
			payload.issues.push({
				expected: "object",
				code: "invalid_type",
				input,
				inst
			});
			return payload;
		}
		payload.value = memo ? memo.alloc(inst, payload, {}, ctx) : {};
		const proms = [];
		const shape = value.shape;
		const abortEarly = ctx?.abortEarly;
		let seen = payload.issues.length;
		for (const key of value.allKeys) {
			if (abortEarly && payload.issues.length !== seen) {
				if (aborted(payload, seen)) break;
				seen = payload.issues.length;
			}
			if (key === "__proto__") continue;
			const el = shape[key];
			const optin = el._zod.optin;
			const optout = el._zod.optout;
			const r = el._zod.run({
				value: input[key],
				issues: []
			}, ctx);
			if (r instanceof Promise) proms.push(r.then((r) => handlePropertyResult(r, payload, key, input, optin, optout)));
			else handlePropertyResult(r, payload, key, input, optin, optout);
		}
		if (!catchall) return proms.length ? Promise.all(proms).then(() => payload) : payload;
		return handleCatchall(proms, input, payload, ctx, _normalized.value, inst, abortEarly === true);
	};
});
const $ZodObjectJIT = /*@__PURE__*/ $constructor("$ZodObjectJIT", (inst, def) => {
	$ZodObject.init(inst, def);
	const superParse = inst._zod.parse;
	const _normalized = cached(() => normalizeDef(def));
	const memo = globalConfig.memoizer;
	const generateFastpass = (shape) => {
		const normalized = _normalized.value;
		const syms = normalized.symbolKeys;
		const doc = new Doc(["payload", "ctx"], {
			shape,
			inst,
			memo,
			syms
		});
		const parseStr = (k) => `shape[${k}]._zod.run({ value: input[${k}], issues: [] }, ctx)`;
		const prefixStr = (id, k) => `
          let ${id}_ab = false;
          for (let i = 0; i < ${id}.issues.length; i++) {
            const iss = ${id}.issues[i];
            iss.path = iss.path ? [${k}, ...iss.path] : [${k}];
            payload.issues.push(iss);
            if (iss.continue !== true) ${id}_ab = true;
          }
          if (${id}_ab && ctx && ctx.abortEarly) {
            payload.value = newResult;
            return payload;
          }`;
		doc.write(`const input = payload.value;`);
		const ids = Object.create(null);
		let counter = 0;
		for (const key of normalized.allKeys) ids[key] = `key_${counter++}`;
		doc.write(memo ? `const newResult = memo.alloc(inst, payload, {}, ctx);` : `const newResult = {};`);
		for (const key of normalized.allKeys) {
			if (key === "__proto__") continue;
			const id = ids[key];
			const k = typeof key === "symbol" ? `syms[${syms.indexOf(key)}]` : esc(key);
			const isPresent = `${k} in input`;
			const schema = shape[key];
			const optin = schema?._zod?.optin;
			const isOptionalIn = optin !== void 0;
			const isOptionalOut = schema?._zod?.optout === "optional";
			doc.write(`const ${id} = ${parseStr(k)};`);
			if (isOptionalIn && isOptionalOut) {
				const assign = optin === "optional" ? `${id}_present` : `${id}.value !== undefined || ${id}_present`;
				doc.write(`
        const ${id}_present = ${isPresent};
        if (!${id}.issues.length || ${id}_present) {
          if (${id}.issues.length) {${prefixStr(id, k)}
          }

          if (${assign}) {
            newResult[${k}] = ${id}.value;
          }
        }

      `);
			} else if (!isOptionalIn) doc.write(`
        const ${id}_present = ${isPresent};
        if (${id}.issues.length) {${prefixStr(id, k)}
        }
        if (!${id}_present && !${id}.issues.length) {
          payload.issues.push({
            code: "invalid_type",
            expected: "nonoptional",
            input: undefined,
            path: [${k}]
          });
          if (ctx && ctx.abortEarly) {
            payload.value = newResult;
            return payload;
          }
        }

        if (${id}_present) {
          newResult[${k}] = ${id}.value;
        }

      `);
			else {
				doc.write(`
        if (${id}.issues.length) {${prefixStr(id, k)}
        }
      `);
				if (optin === "defaulted") doc.write(`newResult[${k}] = ${id}.value;`);
				else doc.write(`
        if (${id}.value !== undefined || ${isPresent}) {
          newResult[${k}] = ${id}.value;
        }
      `);
			}
		}
		doc.write(`payload.value = newResult;`);
		doc.write(`return payload;`);
		return doc.compile();
	};
	let fastpass;
	const isObject$3 = isObject;
	const jit = !globalConfig.jitless;
	const fastEnabled = jit && allowsEval.value;
	const catchall = def.catchall;
	let value;
	inst._zod.parse = (payload, ctx) => {
		value ?? (value = _normalized.value);
		const input = payload.value;
		if (!isObject$3(input)) {
			payload.issues.push({
				expected: "object",
				code: "invalid_type",
				input,
				inst
			});
			return payload;
		}
		if (jit && fastEnabled && ctx?.async === false && ctx.jitless !== true) {
			if (!fastpass) fastpass = generateFastpass(def.shape);
			payload = fastpass(payload, ctx);
			if (!catchall) return payload;
			return handleCatchall([], input, payload, ctx, value, inst, ctx?.abortEarly === true);
		}
		return superParse(payload, ctx);
	};
});
function handleUnionResults(results, final, inst, ctx) {
	for (const result of results) if (result.issues.length === 0) {
		final.value = result.value;
		return final;
	}
	const nonaborted = results.filter((r) => !aborted(r));
	if (nonaborted.length === 1) {
		final.value = nonaborted[0].value;
		return nonaborted[0];
	}
	final.issues.push({
		code: "invalid_union",
		input: final.value,
		inst,
		errors: results.map((result) => result.issues.map((iss) => finalizeIssue(iss, ctx, config())))
	});
	return final;
}
const $ZodUnion = /*@__PURE__*/ $constructor("$ZodUnion", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "optin", (zod) => zod.def.options.some((o) => o._zod.optin === "defaulted") ? "defaulted" : zod.def.options.some((o) => o._zod.optin !== void 0) ? "optional" : void 0);
	defineLazyInternal(inst, "optout", (zod) => zod.def.options.some((o) => o._zod.optout === "optional") ? "optional" : void 0);
	defineLazyInternal(inst, "values", (zod) => {
		if (zod.def.options.every((o) => o._zod.values)) return new Set(zod.def.options.flatMap((option) => Array.from(option._zod.values)));
	});
	defineLazyInternal(inst, "pattern", (zod) => {
		if (zod.def.options.every((o) => o._zod.pattern)) {
			const patterns = zod.def.options.map((o) => o._zod.pattern);
			return new RegExp(`^(${patterns.map((p) => cleanRegex(p.source)).join("|")})$`);
		}
	});
	const first = def.options.length === 1 ? def.options[0]._zod.run : null;
	inst._zod.parse = (payload, ctx) => {
		if (first) return first(payload, ctx);
		let async = false;
		const results = [];
		for (const option of def.options) {
			const result = option._zod.run({
				value: payload.value,
				issues: []
			}, ctx);
			if (result instanceof Promise) {
				results.push(result);
				async = true;
			} else {
				if (result.issues.length === 0) return result;
				results.push(result);
			}
		}
		if (!async) return handleUnionResults(results, payload, inst, ctx);
		return Promise.all(results).then((results) => {
			return handleUnionResults(results, payload, inst, ctx);
		});
	};
});
function discriminatorMap(def) {
	const map = /* @__PURE__ */ new Map();
	for (const option of def.options) {
		const values = option._zod.propValues?.[def.discriminator];
		if (!values || values.size === 0) throw new Error(`Invalid discriminated union option at index "${def.options.indexOf(option)}"`);
		for (const value of values) if (map.has(value)) {
			if (value !== void 0) throw new Error(`Duplicate discriminator value "${String(value)}"`);
			map.set(value, null);
		} else map.set(value, option);
	}
	return map;
}
const $ZodDiscriminatedUnion = /*@__PURE__*/ $constructor("$ZodDiscriminatedUnion", (inst, def) => {
	def.inclusive = false;
	$ZodUnion.init(inst, def);
	const _super = inst._zod.parse;
	defineLazyInternal(inst, "propValues", (zod) => {
		const propValues = {};
		let undefinedCount = 0;
		for (const option of zod.def.options) {
			const pv = option._zod.propValues;
			if (!pv || Object.keys(pv).length === 0) throw new Error(`Invalid discriminated union option at index "${zod.def.options.indexOf(option)}"`);
			if (pv[zod.def.discriminator]?.has(void 0)) undefinedCount++;
			for (const [k, v] of Object.entries(pv)) {
				if (!Object.prototype.hasOwnProperty.call(propValues, k)) assignProp(propValues, k, /* @__PURE__ */ new Set());
				for (const val of v) propValues[k].add(val);
			}
		}
		if (!zod.def.unionFallback && undefinedCount > 1) propValues[zod.def.discriminator]?.delete(void 0);
		return propValues;
	});
	def.options.forEach((option, i) => {
		const propShape = rawShape(option._zod.def);
		if (propShape && !Object.prototype.hasOwnProperty.call(propShape, def.discriminator)) throw new Error(`Invalid discriminated union option at index "${i}"`);
	});
	const disc = cached(() => discriminatorMap(def));
	inst._zod.parse = (payload, ctx) => {
		const input = payload.value;
		if (!isObject(input)) {
			payload.issues.push({
				code: "invalid_type",
				expected: "object",
				input,
				inst
			});
			return payload;
		}
		const value = input?.[def.discriminator];
		const opt = disc.value.get(value);
		if (opt && (value !== void 0 || ctx.direction !== "backward")) return opt._zod.run(payload, ctx);
		if (def.unionFallback || ctx.direction === "backward") return _super(payload, ctx);
		payload.issues.push({
			code: "invalid_union",
			errors: [],
			note: "No matching discriminator",
			discriminator: def.discriminator,
			options: Array.from(disc.value.keys()).filter((value) => disc.value.get(value) !== null),
			input,
			path: [def.discriminator],
			inst
		});
		return payload;
	};
});
const $ZodIntersection = /*@__PURE__*/ $constructor("$ZodIntersection", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.parse = (payload, ctx) => {
		const input = payload.value;
		const left = def.left._zod.run({
			value: input,
			issues: []
		}, ctx);
		const right = def.right._zod.run({
			value: input,
			issues: []
		}, ctx);
		if (left instanceof Promise || right instanceof Promise) return Promise.all([left, right]).then(([left, right]) => {
			return handleIntersectionResults(payload, left, right);
		});
		return handleIntersectionResults(payload, left, right);
	};
});
function mergeValues(a, b) {
	if (a === b) return {
		valid: true,
		data: a
	};
	if (a instanceof Date && b instanceof Date && +a === +b) return {
		valid: true,
		data: a
	};
	if (isPlainObject(a) && isPlainObject(b)) {
		const bKeys = Object.keys(b);
		const sharedKeys = Object.keys(a).filter((key) => bKeys.indexOf(key) !== -1);
		const newObj = {
			...a,
			...b
		};
		if (Object.prototype.hasOwnProperty.call(newObj, "__proto__")) delete newObj.__proto__;
		for (const key of sharedKeys) {
			if (key === "__proto__") continue;
			const sharedValue = mergeValues(a[key], b[key]);
			if (!sharedValue.valid) return {
				valid: false,
				mergeErrorPath: [key, ...sharedValue.mergeErrorPath]
			};
			newObj[key] = sharedValue.data;
		}
		return {
			valid: true,
			data: newObj
		};
	}
	if (Array.isArray(a) && Array.isArray(b)) {
		if (a.length !== b.length) return {
			valid: false,
			mergeErrorPath: []
		};
		const newArray = [];
		for (let index = 0; index < a.length; index++) {
			const itemA = a[index];
			const itemB = b[index];
			const sharedValue = mergeValues(itemA, itemB);
			if (!sharedValue.valid) return {
				valid: false,
				mergeErrorPath: [index, ...sharedValue.mergeErrorPath]
			};
			newArray.push(sharedValue.data);
		}
		return {
			valid: true,
			data: newArray
		};
	}
	return {
		valid: false,
		mergeErrorPath: []
	};
}
function handleIntersectionResults(result, left, right) {
	const unrecKeys = /* @__PURE__ */ new Map();
	let unrecIssue;
	const keyIssues = /* @__PURE__ */ new Map();
	const collect = (iss, side) => {
		let keys;
		if (iss.code === "unrecognized_keys" && !iss.path?.length) {
			unrecIssue ?? (unrecIssue = iss);
			keys = iss.keys;
		} else if (iss.code === "invalid_key" && iss.origin === "record" && iss.path?.length === 1) {
			const k = String(iss.path[0]);
			if (!keyIssues.has(k)) keyIssues.set(k, iss);
			keys = [k];
		} else return false;
		for (const k of keys) {
			if (!unrecKeys.has(k)) unrecKeys.set(k, {});
			unrecKeys.get(k)[side] = true;
		}
		return true;
	};
	for (const iss of left.issues) if (!collect(iss, "l")) result.issues.push(iss);
	for (const iss of right.issues) if (!collect(iss, "r")) result.issues.push(iss);
	const bothKeys = [...unrecKeys].filter(([, f]) => f.l && f.r).map(([k]) => k);
	if (bothKeys.length) {
		const aggregated = unrecIssue ? bothKeys.filter((k) => unrecIssue.keys.includes(k)) : [];
		if (aggregated.length) result.issues.push({
			...unrecIssue,
			keys: aggregated
		});
		for (const k of bothKeys) if (!aggregated.includes(k) && keyIssues.has(k)) result.issues.push(keyIssues.get(k));
	}
	const merged = mergeValues(left.value, right.value);
	if (!merged.valid) {
		if (aborted(result)) return result;
		throw new Error(`Unmergable intersection. Error path: ${JSON.stringify(merged.mergeErrorPath)}`);
	}
	result.value = merged.data;
	return result;
}
const $ZodRecord = /*@__PURE__*/ $constructor("$ZodRecord", (inst, def) => {
	$ZodType.init(inst, def);
	const memo = globalConfig.memoizer;
	memo?.attach(inst);
	inst._zod.parse = (payload, ctx) => {
		const input = payload.value;
		if (!isPlainObject(input)) {
			payload.issues.push({
				expected: "record",
				code: "invalid_type",
				input,
				inst
			});
			return payload;
		}
		const proms = [];
		const values = def.keyType._zod.values;
		if (values && !def.partial) {
			payload.value = memo ? memo.alloc(inst, payload, {}, ctx) : {};
			const recordKeys = /* @__PURE__ */ new Set();
			for (const key of values) if (typeof key === "string" || typeof key === "number" || typeof key === "symbol") {
				recordKeys.add(typeof key === "number" ? key.toString() : key);
				if (key === "__proto__") continue;
				const keyResult = def.keyType._zod.run({
					value: key,
					issues: []
				}, ctx);
				if (keyResult instanceof Promise) throw new Error("Async schemas not supported in object keys currently");
				if (keyResult.issues.length) {
					payload.issues.push({
						code: "invalid_key",
						origin: "record",
						issues: keyResult.issues.map((iss) => finalizeIssue(iss, ctx, config())),
						input: key,
						path: [key],
						inst
					});
					continue;
				}
				const outKey = keyResult.value;
				if (outKey === "__proto__") continue;
				const result = def.valueType._zod.run({
					value: input[key],
					issues: []
				}, ctx);
				if (result instanceof Promise) proms.push(result.then((result) => {
					if (result.issues.length) payload.issues.push(...prefixIssues(key, result.issues));
					payload.value[outKey] = result.value;
				}));
				else {
					if (result.issues.length) payload.issues.push(...prefixIssues(key, result.issues));
					payload.value[outKey] = result.value;
				}
			}
			let unrecognized;
			for (const key in input) if (!recordKeys.has(key)) {
				if (def.mode === "loose") {
					if (key === "__proto__") continue;
					payload.value[key] = input[key];
				} else {
					unrecognized = unrecognized ?? [];
					unrecognized.push(key);
				}
			}
			if (unrecognized && unrecognized.length > 0) payload.issues.push({
				code: "unrecognized_keys",
				input,
				inst,
				keys: unrecognized,
				continue: true
			});
		} else {
			payload.value = memo ? memo.alloc(inst, payload, {}, ctx) : {};
			let unrecognized;
			for (const key of Reflect.ownKeys(input)) {
				if (key === "__proto__") continue;
				if (!Object.prototype.propertyIsEnumerable.call(input, key)) continue;
				let keyResult = def.keyType._zod.run({
					value: key,
					issues: []
				}, ctx);
				if (keyResult instanceof Promise) throw new Error("Async schemas not supported in object keys currently");
				if (typeof key === "string" && number$2.test(key) && keyResult.issues.length) {
					const retryResult = def.keyType._zod.run({
						value: Number(key),
						issues: []
					}, ctx);
					if (retryResult instanceof Promise) throw new Error("Async schemas not supported in object keys currently");
					if (retryResult.issues.length === 0) keyResult = retryResult;
				}
				if (keyResult.issues.length) {
					if (def.mode === "loose") payload.value[key] = input[key];
					else if (values) {
						unrecognized = unrecognized ?? [];
						unrecognized.push(key);
					} else payload.issues.push({
						code: "invalid_key",
						origin: "record",
						issues: keyResult.issues.map((iss) => finalizeIssue(iss, ctx, config())),
						input: key,
						path: [key],
						inst
					});
					continue;
				}
				const outKey = keyResult.value;
				if (outKey === "__proto__") continue;
				const result = def.valueType._zod.run({
					value: input[key],
					issues: []
				}, ctx);
				if (result instanceof Promise) proms.push(result.then((result) => {
					if (result.issues.length) payload.issues.push(...prefixIssues(key, result.issues));
					payload.value[outKey] = result.value;
				}));
				else {
					if (result.issues.length) payload.issues.push(...prefixIssues(key, result.issues));
					payload.value[outKey] = result.value;
				}
			}
			if (unrecognized && unrecognized.length > 0) payload.issues.push({
				code: "unrecognized_keys",
				input,
				inst,
				keys: unrecognized,
				continue: true
			});
		}
		if (proms.length) return Promise.all(proms).then(() => payload);
		return payload;
	};
});
const $ZodEnum = /*@__PURE__*/ $constructor("$ZodEnum", (inst, def) => {
	$ZodType.init(inst, def);
	const values = getEnumValues(def.entries);
	const valuesSet = new Set(values);
	inst._zod.values = valuesSet;
	defineLazyInternal(inst, "pattern", (zod) => {
		const patternValues = getEnumValues(zod.def.entries).filter((k) => propertyKeyTypes.has(typeof k));
		return new RegExp(patternValues.length ? `^(${patternValues.map((o) => escapeRegex(o.toString())).join("|")})$` : "^[^\\s\\S]$");
	});
	inst._zod.parse = (payload, _ctx) => {
		const input = payload.value;
		if (valuesSet.has(input)) return payload;
		payload.issues.push({
			code: "invalid_value",
			values,
			input,
			inst
		});
		return payload;
	};
});
const $ZodLiteral = /*@__PURE__*/ $constructor("$ZodLiteral", (inst, def) => {
	$ZodType.init(inst, def);
	const values = new Set(def.values);
	inst._zod.values = values;
	defineLazyInternal(inst, "pattern", (zod) => {
		const vals = zod.def.values;
		return new RegExp(vals.length ? `^(${vals.map((o) => typeof o === "string" ? escapeRegex(o) : o ? escapeRegex(o.toString()) : String(o)).join("|")})$` : "^[^\\s\\S]$");
	});
	inst._zod.parse = (payload, _ctx) => {
		const input = payload.value;
		if (values.has(input)) return payload;
		payload.issues.push({
			code: "invalid_value",
			values: def.values,
			input,
			inst
		});
		return payload;
	};
});
const $ZodTransform = /*@__PURE__*/ $constructor("$ZodTransform", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.optin = "optional";
	globalConfig.memoizer?.guard(inst);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") throw new $ZodEncodeError(inst.constructor.name);
		const _out = def.transform(payload.value, payload);
		if (ctx.async) return (_out instanceof Promise ? _out : Promise.resolve(_out)).then((output) => {
			payload.value = output;
			return payload;
		});
		if (_out instanceof Promise) throw new $ZodAsyncError();
		payload.value = _out;
		return payload;
	};
});
function handleOptionalResult(payload, result) {
	payload.value = result.issues.length ? void 0 : result.value;
	return payload;
}
const $ZodOptional = /*@__PURE__*/ $constructor("$ZodOptional", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "optin", (zod) => zod.def.innerType._zod.optin === "defaulted" ? "defaulted" : "optional");
	inst._zod.optout = "optional";
	defineLazyInternal(inst, "values", (zod) => {
		const values = zod.def.innerType._zod.values;
		return values ? /* @__PURE__ */ new Set([...values, void 0]) : void 0;
	});
	defineLazyInternal(inst, "pattern", (zod) => {
		const pattern = zod.def.innerType._zod.pattern;
		return pattern ? new RegExp(`^(${cleanRegex(pattern.source)})?$`) : void 0;
	});
	inst._zod.parse = (payload, ctx) => {
		if (payload.value === void 0) {
			if (def.innerType._zod.optin !== "defaulted") return payload;
			const result = def.innerType._zod.run({
				value: payload.value,
				issues: []
			}, ctx);
			if (result instanceof Promise) return result.then((result) => handleOptionalResult(payload, result));
			return handleOptionalResult(payload, result);
		}
		return def.innerType._zod.run(payload, ctx);
	};
});
const $ZodExactOptional = /*@__PURE__*/ $constructor("$ZodExactOptional", (inst, def) => {
	$ZodOptional.init(inst, def);
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	defineLazyInternal(inst, "pattern", (zod) => zod.def.innerType._zod.pattern);
	inst._zod.parse = (payload, ctx) => {
		return def.innerType._zod.run(payload, ctx);
	};
});
const $ZodNullable = /*@__PURE__*/ $constructor("$ZodNullable", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "optin", (zod) => zod.def.innerType._zod.optin);
	defineLazyInternal(inst, "optout", (zod) => zod.def.innerType._zod.optout);
	defineLazyInternal(inst, "pattern", (zod) => {
		const pattern = zod.def.innerType._zod.pattern;
		return pattern ? new RegExp(`^(${cleanRegex(pattern.source)}|null)$`) : void 0;
	});
	defineLazyInternal(inst, "values", (zod) => {
		return zod.def.innerType._zod.values ? /* @__PURE__ */ new Set([...zod.def.innerType._zod.values, null]) : void 0;
	});
	inst._zod.parse = (payload, ctx) => {
		if (payload.value === null) return payload;
		return def.innerType._zod.run(payload, ctx);
	};
});
const $ZodDefault = /*@__PURE__*/ $constructor("$ZodDefault", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.optin = "defaulted";
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") return def.innerType._zod.run(payload, ctx);
		if (payload.value === void 0) {
			payload.value = def.defaultValue;
			/**
			* $ZodDefault returns the default value immediately in forward direction.
			* It doesn't pass the default value into the validator ("prefault"). There's no reason to pass the default value through validation. The validity of the default is enforced by TypeScript statically. Otherwise, it's the responsibility of the user to ensure the default is valid. In the case of pipes with divergent in/out types, you can specify the default on the `in` schema of your ZodPipe to set a "prefault" for the pipe.   */
			return payload;
		}
		const result = def.innerType._zod.run(payload, ctx);
		if (result instanceof Promise) return result.then((result) => handleDefaultResult(result, def));
		return handleDefaultResult(result, def);
	};
});
function handleDefaultResult(payload, def) {
	if (payload.value === void 0) payload.value = def.defaultValue;
	return payload;
}
const $ZodPrefault = /*@__PURE__*/ $constructor("$ZodPrefault", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.optin = "defaulted";
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") return def.innerType._zod.run(payload, ctx);
		if (payload.value === void 0) payload.value = def.defaultValue;
		return def.innerType._zod.run(payload, ctx);
	};
});
const $ZodNonOptional = /*@__PURE__*/ $constructor("$ZodNonOptional", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "values", (zod) => {
		const v = zod.def.innerType._zod.values;
		return v ? new Set([...v].filter((x) => x !== void 0)) : void 0;
	});
	inst._zod.parse = (payload, ctx) => {
		const result = def.innerType._zod.run(payload, ctx);
		if (result instanceof Promise) return result.then((result) => handleNonOptionalResult(result, inst));
		return handleNonOptionalResult(result, inst);
	};
});
function handleNonOptionalResult(payload, inst) {
	if (!payload.issues.length && payload.value === void 0) payload.issues.push({
		code: "invalid_type",
		expected: "nonoptional",
		input: payload.value,
		inst
	});
	return payload;
}
function handleCatchResult(payload, result, def, ctx) {
	if (!result.issues.length) {
		payload.value = result.value;
		if (result.memo) payload.memo = true;
		return payload;
	}
	payload.value = def.catchValue({
		...result,
		value: payload.value,
		error: { issues: result.issues.map((iss) => finalizeIssue(iss, ctx, config())) },
		input: payload.value
	});
	return payload;
}
const $ZodCatch = /*@__PURE__*/ $constructor("$ZodCatch", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "optin", (zod) => zod.def.innerType._zod.optin === "defaulted" ? "defaulted" : "optional");
	defineLazyInternal(inst, "optout", (zod) => zod.def.innerType._zod.optout);
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") return def.innerType._zod.run(payload, ctx);
		const result = def.innerType._zod.run({
			value: payload.value,
			issues: []
		}, ctx);
		if (result instanceof Promise) return result.then((result) => handleCatchResult(payload, result, def, ctx));
		return handleCatchResult(payload, result, def, ctx);
	};
});
const $ZodPipe = /*@__PURE__*/ $constructor("$ZodPipe", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "values", (zod) => zod.def.in._zod.values);
	defineLazyInternal(inst, "optin", (zod) => zod.def.in._zod.optin);
	defineLazyInternal(inst, "optout", (zod) => zod.def.out._zod.optout);
	defineLazyInternal(inst, "propValues", (zod) => zod.def.in._zod.propValues);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") {
			const right = def.out._zod.run(payload, ctx);
			if (right instanceof Promise) return right.then((right) => handlePipeResult(right, def.in, ctx));
			return handlePipeResult(right, def.in, ctx);
		}
		const left = def.in._zod.run(payload, ctx);
		if (left instanceof Promise) return left.then((left) => handlePipeResult(left, def.out, ctx));
		return handlePipeResult(left, def.out, ctx);
	};
});
function handlePipeResult(left, next, ctx) {
	if (left.issues.some((iss) => iss.code !== "unrecognized_keys")) {
		left.aborted = true;
		return left;
	}
	return next._zod.run({
		value: left.value,
		issues: left.issues
	}, ctx);
}
const $ZodPreprocess = /*@__PURE__*/ $constructor("$ZodPreprocess", (inst, def) => {
	$ZodPipe.init(inst, def);
});
const $ZodReadonly = /*@__PURE__*/ $constructor("$ZodReadonly", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "propValues", (zod) => zod.def.innerType._zod.propValues);
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	defineLazyInternal(inst, "optin", (zod) => zod.def.innerType?._zod?.optin);
	defineLazyInternal(inst, "optout", (zod) => zod.def.innerType?._zod?.optout);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") return def.innerType._zod.run(payload, ctx);
		const result = def.innerType._zod.run(payload, ctx);
		if (result instanceof Promise) return result.then(handleReadonlyResult);
		return handleReadonlyResult(result);
	};
});
function handleReadonlyResult(payload) {
	if (!payload.memo) payload.value = Object.freeze(payload.value);
	return payload;
}
const $ZodCustom = /*@__PURE__*/ $constructor("$ZodCustom", (inst, def) => {
	$ZodCheck.init(inst, def);
	$ZodType.init(inst, def);
	inst._zod.parse = (payload, _) => {
		return payload;
	};
	inst._zod.check = (payload) => {
		const input = payload.value;
		const r = def.fn(input);
		if (r instanceof Promise) return r.then((r) => handleRefineResult(r, payload, input, inst));
		handleRefineResult(r, payload, input, inst);
	};
});
function handleRefineResult(result, payload, input, inst) {
	if (!result) {
		const _iss = {
			code: "custom",
			input,
			inst,
			path: [...inst._zod.def.path ?? []],
			continue: !inst._zod.def.abort
		};
		if (inst._zod.def.params) _iss.params = inst._zod.def.params;
		payload.issues.push(issue(_iss));
	}
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/memoizer.js
var $ZodCyclicError = class extends Error {
	constructor() {
		super(`Cannot parse a reference cycle that closes through a transform`);
		this.name = "ZodCyclicError";
	}
};
/** Keyed off the context object every schema in one parse call already shares. */
const STATE = "~memo";
const NO_ISSUES = [];
function isRef(value) {
	return value !== null && typeof value === "object";
}
function cloneIssues(issues) {
	return issues.map((iss) => iss.path ? {
		...iss,
		path: iss.path.slice()
	} : { ...iss });
}
const recursive = /*@__PURE__*/ new WeakMap();
/** What the walk established, in order of certainty: ordered so the strongest answer among children wins. */
const NONE = 0;
const ASSUMED = 1;
const PROVEN = 2;
/** Whether this schema's subtree contains a cycle, so one parse can re-enter it. */
function isRecursive(inst, stack, resolve) {
	const cached = recursive.get(inst);
	if (cached !== void 0) return cached ? PROVEN : NONE;
	if (stack.has(inst)) return PROVEN;
	stack.add(inst);
	let result = NONE;
	const check = (child) => {
		if (result !== PROVEN && child?._zod) {
			const answer = isRecursive(child, stack, resolve);
			if (answer > result) result = answer;
		}
	};
	const shape = (sh, spread) => {
		let answer = NONE;
		for (const key of Reflect.ownKeys(sh)) {
			const desc = Object.getOwnPropertyDescriptor(sh, key);
			if (spread && !desc.enumerable) continue;
			const child = desc.get ? ASSUMED : desc.value?._zod ? isRecursive(desc.value, stack, resolve) : NONE;
			if (child > answer) answer = child;
		}
		return answer;
	};
	const merge = (answer) => {
		if (answer > result) result = answer;
	};
	const def = inst._zod.def;
	switch (def.type) {
		case "object": {
			const raw = rawShape(def);
			merge(raw ? shape(raw, true) : ASSUMED);
			check(def.catchall);
			break;
		}
		case "array":
			check(def.element);
			break;
		case "tuple":
			for (const el of def.items) check(el);
			check(def.rest);
			break;
		case "record":
		case "map":
			check(def.keyType);
			check(def.valueType);
			break;
		case "set":
			check(def.valueType);
			break;
		case "union":
			for (const el of def.options) check(el);
			break;
		case "intersection":
			check(def.left);
			check(def.right);
			break;
		case "optional":
		case "nullable":
		case "default":
		case "prefault":
		case "catch":
		case "readonly":
		case "nonoptional":
		case "promise":
		case "success":
			check(def.innerType);
			break;
		case "pipe":
			check(def.in);
			check(def.out);
			break;
		case "function":
			check(def.input);
			check(def.output);
			break;
		case "lazy": {
			const inner = def._cachedInner ?? (resolve ? inst._zod.innerType : void 0);
			merge(inner ? isRecursive(inner, stack, false) : ASSUMED);
			break;
		}
		case "template_literal":
		case "string":
		case "number":
		case "int":
		case "boolean":
		case "bigint":
		case "symbol":
		case "undefined":
		case "null":
		case "void":
		case "never":
		case "any":
		case "unknown":
		case "date":
		case "nan":
		case "enum":
		case "literal":
		case "file":
		case "transform":
		case "custom": break;
		default: for (const key in def) {
			const desc = Object.getOwnPropertyDescriptor(def, key);
			if (!desc || desc.get) continue;
			const value = desc.value;
			if (!value || typeof value !== "object") continue;
			if (value._zod) check(value);
			else if (Array.isArray(value)) for (const el of value) check(el);
		}
	}
	stack.delete(inst);
	return settle(inst, result);
}
/** An assumed answer must not outlive the resolution that settles it, so only a certain one is cached. */
function settle(inst, answer) {
	if (answer !== ASSUMED) recursive.set(inst, answer === PROVEN);
	return answer;
}
function bucketFor(state, inst) {
	let bucket = state.buckets.get(inst);
	if (!bucket) {
		bucket = /* @__PURE__ */ new WeakMap();
		state.buckets.set(inst, bucket);
	}
	return bucket;
}
let handoff;
const open = [];
const memo = {
	alloc(_inst, payload, empty) {
		const bucket = handoff;
		if (!bucket) return empty;
		handoff = void 0;
		const entry = {
			value: empty,
			issues: null
		};
		bucket.set(payload.value, entry);
		open.push(entry);
		return empty;
	},
	guard(inst) {
		var _a;
		(_a = inst._zod).deferred ?? (_a.deferred = []);
		inst._zod.deferred.push(() => {
			const base = inst._zod.parse;
			const wrapped = (payload, ctx) => {
				if (ctx.direction !== "backward" && isBackEdge(ctx, payload.value)) throw new $ZodCyclicError();
				return base(payload, ctx);
			};
			inst._zod.parse = wrapped;
			if (inst._zod.run === base) inst._zod.run = wrapped;
		});
	},
	attach(inst) {
		var _a;
		let isRecursiveInst;
		let rechecked = false;
		let lastCtx;
		let lastBucket;
		(_a = inst._zod).deferred ?? (_a.deferred = []);
		inst._zod.deferred.push(() => {
			const base = inst._zod.parse;
			const wrapped = (payload, ctx) => {
				if (isRecursiveInst === void 0) {
					const walked = isRecursive(inst, /* @__PURE__ */ new Set(), false);
					if (walked === NONE) {
						inst._zod.parse = base;
						if (inst._zod.run === wrapped) inst._zod.run = base;
						return base(payload, ctx);
					}
					if (walked === PROVEN || rechecked) isRecursiveInst = true;
					else rechecked = true;
				}
				const input = payload.value;
				if (!isRef(input)) return base(payload, ctx);
				let state = ctx[STATE];
				if (!state) {
					state = {
						buckets: /* @__PURE__ */ new WeakMap(),
						backEdges: void 0
					};
					ctx[STATE] = state;
				}
				let bucket;
				if (lastCtx === ctx) bucket = lastBucket;
				else {
					bucket = bucketFor(state, inst);
					lastCtx = ctx;
					lastBucket = bucket;
				}
				const hit = bucket.get(input);
				if (hit) {
					payload.value = hit.value;
					if (hit.issues) {
						if (hit.issues.length) payload.issues.push(...cloneIssues(hit.issues));
					} else {
						payload.memo = true;
						state.backEdges ?? (state.backEdges = /* @__PURE__ */ new WeakSet());
						state.backEdges.add(hit.value);
					}
					return payload;
				}
				handoff = bucket;
				const depth = open.length;
				const result = base(payload, ctx);
				handoff = void 0;
				const entry = open.length > depth ? open.pop() : void 0;
				if (result instanceof Promise) return result.then((r) => {
					if (entry) entry.issues = r.issues.length ? cloneIssues(r.issues) : NO_ISSUES;
					return r;
				});
				if (entry) entry.issues = result.issues.length ? cloneIssues(result.issues) : NO_ISSUES;
				return result;
			};
			inst._zod.parse = wrapped;
			if (inst._zod.run === base) inst._zod.run = wrapped;
		});
	}
};
/** The memoizer that gives containers cycle support. `zod` installs it by default; `zod/mini` opts in with `config({ memoizer: memoizer() })`. */
function memoizer() {
	return memo;
}
/** Whether this value is a node a back-edge resolved to before it finished. */
function isBackEdge(ctx, value) {
	const backEdges = ctx[STATE]?.backEdges;
	return backEdges !== void 0 && isRef(value) && backEdges.has(value);
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/locales/en.js
const error = () => {
	const Sizable = {
		string: {
			unit: "characters",
			verb: "to have"
		},
		file: {
			unit: "bytes",
			verb: "to have"
		},
		array: {
			unit: "items",
			verb: "to have"
		},
		set: {
			unit: "items",
			verb: "to have"
		},
		map: {
			unit: "entries",
			verb: "to have"
		}
	};
	function getSizing(origin) {
		return Sizable[origin] ?? null;
	}
	const FormatDictionary = {
		regex: "input",
		email: "email address",
		url: "URL",
		emoji: "emoji",
		uuid: "UUID",
		uuidv4: "UUIDv4",
		uuidv6: "UUIDv6",
		nanoid: "nanoid",
		guid: "GUID",
		cuid: "cuid",
		cuid2: "cuid2",
		ulid: "ULID",
		xid: "XID",
		ksuid: "KSUID",
		datetime: "ISO datetime",
		date: "ISO date",
		time: "ISO time",
		duration: "ISO duration",
		ipv4: "IPv4 address",
		ipv6: "IPv6 address",
		mac: "MAC address",
		cidrv4: "IPv4 range",
		cidrv6: "IPv6 range",
		base64: "base64-encoded string",
		base64url: "base64url-encoded string",
		json_string: "JSON string",
		e164: "E.164 number",
		currency_code: "currency code",
		credit_card: "credit card number",
		iban: "IBAN",
		jwt: "JWT",
		template_literal: "input"
	};
	const TypeDictionary = { nan: "NaN" };
	function getTypeName(type, input) {
		if (type === "number" && typeof input === "number" && !Number.isFinite(input)) return String(input);
		return TypeDictionary[type] ?? type;
	}
	return (issue) => {
		switch (issue.code) {
			case "invalid_type": return `Invalid input: expected ${getTypeName(issue.expected)}, received ${getTypeName(parsedType(issue.input), issue.input)}`;
			case "invalid_value":
				if (issue.values.length === 1) return `Invalid input: expected ${stringifyPrimitive(issue.values[0])}`;
				return `Invalid option: expected one of ${joinValues(issue.values, "|")}`;
			case "too_big": {
				const adj = issue.exact ? "exactly " : issue.inclusive ? "<=" : "<";
				const sizing = getSizing(issue.origin);
				if (sizing) return `Too big: expected ${issue.origin ?? "value"} to have ${adj}${issue.maximum.toString()} ${sizing.unit ?? "elements"}`;
				return `Too big: expected ${issue.origin ?? "value"} to be ${adj}${issue.maximum.toString()}`;
			}
			case "too_small": {
				const adj = issue.exact ? "exactly " : issue.inclusive ? ">=" : ">";
				const sizing = getSizing(issue.origin);
				if (sizing) return `Too small: expected ${issue.origin} to have ${adj}${issue.minimum.toString()} ${sizing.unit}`;
				return `Too small: expected ${issue.origin} to be ${adj}${issue.minimum.toString()}`;
			}
			case "invalid_format": {
				const _issue = issue;
				if (_issue.format === "starts_with") return `Invalid string: must start with "${_issue.prefix}"`;
				if (_issue.format === "ends_with") return `Invalid string: must end with "${_issue.suffix}"`;
				if (_issue.format === "includes") return `Invalid string: must include "${_issue.includes}"`;
				if (_issue.format === "regex") return `Invalid string: must match pattern ${_issue.pattern}`;
				return `Invalid ${FormatDictionary[_issue.format] ?? issue.format}`;
			}
			case "not_multiple_of": return `Invalid number: must be a multiple of ${issue.divisor}`;
			case "unrecognized_keys": return `Unrecognized key${issue.keys.length > 1 ? "s" : ""}: ${joinValues(issue.keys, ", ")}`;
			case "invalid_key": return `Invalid key in ${issue.origin}`;
			case "invalid_union":
				if (issue.options && Array.isArray(issue.options) && issue.options.length > 0) return `Invalid discriminator value. Expected ${issue.options.map((o) => `'${o}'`).join(" | ")}`;
				if (issue.inclusive === false) return "Invalid input: more than one option matched";
				return "Invalid input";
			case "invalid_element": return `Invalid value in ${issue.origin}`;
			default: return `Invalid input`;
		}
	};
};
function en_default() {
	return { localeError: error() };
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/registries.js
var _a;
var $ZodRegistry = class {
	constructor() {
		this._map = /* @__PURE__ */ new WeakMap();
		this._idmap = /* @__PURE__ */ new Map();
	}
	add(schema, ..._meta) {
		const meta = _meta[0];
		this._map.set(schema, meta);
		if (meta && typeof meta === "object" && "id" in meta) this._idmap.set(meta.id, schema);
		return this;
	}
	clear() {
		this._map = /* @__PURE__ */ new WeakMap();
		this._idmap = /* @__PURE__ */ new Map();
		return this;
	}
	remove(schema) {
		const meta = this._map.get(schema);
		if (meta && typeof meta === "object" && "id" in meta) this._idmap.delete(meta.id);
		this._map.delete(schema);
		return this;
	}
	get(schema) {
		const p = schema._zod.parent;
		if (p) {
			const pm = { ...this.get(p) ?? {} };
			delete pm.id;
			const f = {
				...pm,
				...this._map.get(schema)
			};
			return Object.keys(f).length ? f : void 0;
		}
		return this._map.get(schema);
	}
	has(schema) {
		return this._map.has(schema);
	}
};
function registry() {
	return new $ZodRegistry();
}
(_a = globalThis).__zod_globalRegistry ?? (_a.__zod_globalRegistry = registry());
const globalRegistry = globalThis.__zod_globalRegistry;
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/api.js
function snapshotChecks(def) {
	if (def.checks) def.checks = [...def.checks];
	return def;
}
// @__NO_SIDE_EFFECTS__
function _string(Class, params) {
	return new Class(snapshotChecks({
		type: "string",
		...normalizeParams(params)
	}));
}
// @__NO_SIDE_EFFECTS__
function _email(Class, params) {
	return new Class({
		type: "string",
		format: "email",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _guid(Class, params) {
	return new Class({
		type: "string",
		format: "guid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uuid(Class, params) {
	return new Class({
		type: "string",
		format: "uuid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uuidv4(Class, params) {
	return new Class({
		type: "string",
		format: "uuid",
		check: "string_format",
		abort: false,
		version: "v4",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uuidv6(Class, params) {
	return new Class({
		type: "string",
		format: "uuid",
		check: "string_format",
		abort: false,
		version: "v6",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uuidv7(Class, params) {
	return new Class({
		type: "string",
		format: "uuid",
		check: "string_format",
		abort: false,
		version: "v7",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _url(Class, params) {
	return new Class({
		type: "string",
		format: "url",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _emoji(Class, params) {
	return new Class({
		type: "string",
		format: "emoji",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _nanoid(Class, params) {
	return new Class({
		type: "string",
		format: "nanoid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
/**
* @deprecated CUID v1 is deprecated by its authors due to information leakage
* (timestamps embedded in the id). Use {@link _cuid2} instead.
* See https://github.com/paralleldrive/cuid.
*/
// @__NO_SIDE_EFFECTS__
function _cuid(Class, params) {
	return new Class({
		type: "string",
		format: "cuid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _cuid2(Class, params) {
	return new Class({
		type: "string",
		format: "cuid2",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _ulid(Class, params) {
	return new Class({
		type: "string",
		format: "ulid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _xid(Class, params) {
	return new Class({
		type: "string",
		format: "xid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _ksuid(Class, params) {
	return new Class({
		type: "string",
		format: "ksuid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _ipv4(Class, params) {
	return new Class({
		type: "string",
		format: "ipv4",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _ipv6(Class, params) {
	return new Class({
		type: "string",
		format: "ipv6",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _cidrv4(Class, params) {
	return new Class({
		type: "string",
		format: "cidrv4",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _cidrv6(Class, params) {
	return new Class({
		type: "string",
		format: "cidrv6",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _base64(Class, params) {
	return new Class({
		type: "string",
		format: "base64",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _base64url(Class, params) {
	return new Class({
		type: "string",
		format: "base64url",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _e164(Class, params) {
	return new Class({
		type: "string",
		format: "e164",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _jwt(Class, params) {
	return new Class({
		type: "string",
		format: "jwt",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _isoDateTime(Class, params) {
	return new Class({
		type: "string",
		format: "datetime",
		check: "string_format",
		offset: false,
		local: false,
		precision: null,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _isoDate(Class, params) {
	return new Class({
		type: "string",
		format: "date",
		check: "string_format",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _isoTime(Class, params) {
	return new Class({
		type: "string",
		format: "time",
		check: "string_format",
		precision: null,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _isoDuration(Class, params) {
	return new Class({
		type: "string",
		format: "duration",
		check: "string_format",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _number(Class, params) {
	return new Class(snapshotChecks({
		type: "number",
		checks: [],
		...normalizeParams(params)
	}));
}
// @__NO_SIDE_EFFECTS__
function _coercedNumber(Class, params) {
	return new Class(snapshotChecks({
		type: "number",
		coerce: true,
		checks: [],
		...normalizeParams(params)
	}));
}
// @__NO_SIDE_EFFECTS__
function _int(Class, params) {
	return new Class({
		type: "number",
		check: "number_format",
		abort: false,
		format: "safeint",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _boolean(Class, params) {
	return new Class({
		type: "boolean",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _null$1(Class, params) {
	return new Class({
		type: "null",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _any(Class) {
	return new Class({ type: "any" });
}
// @__NO_SIDE_EFFECTS__
function _unknown(Class) {
	return new Class({ type: "unknown" });
}
// @__NO_SIDE_EFFECTS__
function _never(Class, params) {
	return new Class({
		type: "never",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _lt(value, params) {
	return new $ZodCheckLessThan({
		check: "less_than",
		...normalizeParams(params),
		value,
		inclusive: false
	});
}
// @__NO_SIDE_EFFECTS__
function _lte(value, params) {
	return new $ZodCheckLessThan({
		check: "less_than",
		...normalizeParams(params),
		value,
		inclusive: true
	});
}
// @__NO_SIDE_EFFECTS__
function _gt(value, params) {
	return new $ZodCheckGreaterThan({
		check: "greater_than",
		...normalizeParams(params),
		value,
		inclusive: false
	});
}
// @__NO_SIDE_EFFECTS__
function _gte(value, params) {
	return new $ZodCheckGreaterThan({
		check: "greater_than",
		...normalizeParams(params),
		value,
		inclusive: true
	});
}
// @__NO_SIDE_EFFECTS__
function _multipleOf(value, params) {
	return new $ZodCheckMultipleOf({
		check: "multiple_of",
		...normalizeParams(params),
		value
	});
}
// @__NO_SIDE_EFFECTS__
function _maxLength(maximum, params) {
	return new $ZodCheckMaxLength({
		check: "max_length",
		...normalizeParams(params),
		maximum
	});
}
// @__NO_SIDE_EFFECTS__
function _minLength(minimum, params) {
	return new $ZodCheckMinLength({
		check: "min_length",
		...normalizeParams(params),
		minimum
	});
}
// @__NO_SIDE_EFFECTS__
function _length(length, params) {
	return new $ZodCheckLengthEquals({
		check: "length_equals",
		...normalizeParams(params),
		length
	});
}
// @__NO_SIDE_EFFECTS__
function _regex(pattern, params) {
	return new $ZodCheckRegex({
		check: "string_format",
		format: "regex",
		...normalizeParams(params),
		pattern
	});
}
// @__NO_SIDE_EFFECTS__
function _lowercase(params) {
	return new $ZodCheckLowerCase({
		check: "string_format",
		format: "lowercase",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uppercase(params) {
	return new $ZodCheckUpperCase({
		check: "string_format",
		format: "uppercase",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _includes(includes, params) {
	return new $ZodCheckIncludes({
		check: "string_format",
		format: "includes",
		...normalizeParams(params),
		includes
	});
}
// @__NO_SIDE_EFFECTS__
function _startsWith(prefix, params) {
	return new $ZodCheckStartsWith({
		check: "string_format",
		format: "starts_with",
		...normalizeParams(params),
		prefix
	});
}
// @__NO_SIDE_EFFECTS__
function _endsWith(suffix, params) {
	return new $ZodCheckEndsWith({
		check: "string_format",
		format: "ends_with",
		...normalizeParams(params),
		suffix
	});
}
// @__NO_SIDE_EFFECTS__
function _overwrite(tx) {
	return new $ZodCheckOverwrite({
		check: "overwrite",
		tx
	});
}
// @__NO_SIDE_EFFECTS__
function _normalize(form) {
	return /* @__PURE__ */ _overwrite((input) => input.normalize(form));
}
// @__NO_SIDE_EFFECTS__
function _trim() {
	return /* @__PURE__ */ _overwrite((input) => input.trim());
}
// @__NO_SIDE_EFFECTS__
function _toLowerCase() {
	return /* @__PURE__ */ _overwrite((input) => input.toLowerCase());
}
// @__NO_SIDE_EFFECTS__
function _toUpperCase() {
	return /* @__PURE__ */ _overwrite((input) => input.toUpperCase());
}
// @__NO_SIDE_EFFECTS__
function _slugify() {
	return /* @__PURE__ */ _overwrite((input) => slugify(input));
}
// @__NO_SIDE_EFFECTS__
function _array(Class, element, params) {
	return new Class({
		type: "array",
		element,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _custom(Class, fn, _params) {
	const norm = normalizeParams(_params);
	norm.abort ?? (norm.abort = true);
	return new Class({
		type: "custom",
		check: "custom",
		fn,
		...norm
	});
}
// @__NO_SIDE_EFFECTS__
function _refine(Class, fn, _params) {
	return new Class({
		type: "custom",
		check: "custom",
		fn,
		...normalizeParams(_params)
	});
}
// @__NO_SIDE_EFFECTS__
function _superRefine(fn, params) {
	const ch = /* @__PURE__ */ _check((payload) => {
		payload.addIssue = (issue$2) => {
			if (typeof issue$2 === "string") payload.issues.push(issue(issue$2, payload.value, ch._zod.def));
			else {
				const _issue = issue$2;
				if (_issue.fatal) _issue.continue = false;
				_issue.code ?? (_issue.code = "custom");
				if (!("input" in _issue)) _issue.input = payload.value;
				_issue.inst ?? (_issue.inst = ch);
				_issue.continue ?? (_issue.continue = !ch._zod.def.abort);
				payload.issues.push(issue(_issue));
			}
		};
		return fn(payload.value, payload);
	}, params);
	return ch;
}
// @__NO_SIDE_EFFECTS__
function _check(fn, params) {
	const ch = new $ZodCheck({
		check: "custom",
		...normalizeParams(params)
	});
	ch._zod.check = fn;
	return ch;
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/to-json-schema.js
function assignProps(target, ...sources) {
	for (const source of sources) for (const key of Reflect.ownKeys(source)) if (Object.prototype.propertyIsEnumerable.call(source, key)) assignProp(target, key, source[key]);
	return target;
}
function initializeContext(params) {
	let target = params?.target ?? "draft-2020-12";
	if (target === "draft-4") target = "draft-04";
	if (target === "draft-7") target = "draft-07";
	return {
		processors: params.processors ?? {},
		metadataRegistry: params?.metadata ?? globalRegistry,
		target,
		unrepresentable: params?.unrepresentable ?? "throw",
		override: params?.override ?? (() => {}),
		io: params?.io ?? "output",
		counter: 0,
		seen: /* @__PURE__ */ new Map(),
		sharedDefsExtractedFor: void 0,
		sharedEmitDoneFor: void 0,
		cycles: params?.cycles ?? "ref",
		reused: params?.reused ?? "inline",
		intersections: [],
		deferred: [],
		external: params?.external ?? void 0
	};
}
/**
* Applies the `unrepresentable` setting at a site that has no JSON Schema equivalent. Throws
* `message` unless the setting (or the handler's return value) says otherwise. Returns `true` if a
* custom JSON Schema was written into `json`, in which case the caller must not write its own.
*/
function handleUnrepresentable(schema, ctx, json, params, message) {
	const result = typeof ctx.unrepresentable === "function" ? ctx.unrepresentable({
		zodSchema: schema,
		path: params.path,
		message
	}) : ctx.unrepresentable;
	if (result === "any") return false;
	if (result === void 0 || result === "throw") throw new Error(message);
	Object.assign(json, result);
	return true;
}
function processSchema(schema, ctx, _params = {
	path: [],
	schemaPath: []
}) {
	var _a;
	const def = schema._zod.def;
	const seen = ctx.seen.get(schema);
	if (seen) {
		seen.count++;
		if (_params.schemaPath.includes(schema)) seen.cycle = _params.path;
		return seen.schema;
	}
	const result = {
		schema: {},
		count: 1,
		cycle: void 0,
		path: _params.path
	};
	ctx.seen.set(schema, result);
	ctx.sharedDefsExtractedFor = void 0;
	ctx.sharedEmitDoneFor = void 0;
	const overrideSchema = schema._zod.toJSONSchema?.();
	if (overrideSchema) result.schema = overrideSchema;
	else {
		const params = {
			..._params,
			schemaPath: [..._params.schemaPath, schema],
			path: _params.path
		};
		if (schema._zod.processJSONSchema) schema._zod.processJSONSchema(ctx, result.schema, params);
		else {
			const _json = result.schema;
			const processor = ctx.processors[def.type];
			if (!processor) throw new Error(`[toJSONSchema]: Non-representable type encountered: ${def.type}`);
			processor(schema, ctx, _json, params);
		}
		const parent = schema._zod.parent;
		if (parent) {
			if (!result.ref) result.ref = parent;
			processSchema(parent, ctx, params);
			ctx.seen.get(parent).isParent = true;
		}
	}
	const meta = ctx.metadataRegistry.get(schema);
	if (meta) assignProps(result.schema, meta);
	if (ctx.io === "input" && isTransforming(schema)) {
		delete result.schema.examples;
		delete result.schema.default;
	}
	if (ctx.io === "input" && "_prefault" in result.schema) (_a = result.schema).default ?? (_a.default = result.schema._prefault);
	delete result.schema._prefault;
	return ctx.seen.get(schema).schema;
}
function encodeJSONPointerSegment(segment) {
	return segment.replace(/~/g, "~0").replace(/\//g, "~1");
}
function extractDefs(ctx, schema) {
	const root = ctx.seen.get(schema);
	if (!root) throw new Error("Unprocessed schema. This is a bug in Zod.");
	if (ctx.external && ctx.sharedDefsExtractedFor === ctx.external) return;
	const idToSchema = /* @__PURE__ */ new Map();
	for (const entry of ctx.seen.entries()) {
		const id = ctx.metadataRegistry.get(entry[0])?.id;
		if (id) {
			const existing = idToSchema.get(id);
			if (existing && existing !== entry[0]) throw new Error(`Duplicate schema id "${id}" detected during JSON Schema conversion. Two different schemas cannot share the same id when converted together.`);
			idToSchema.set(id, entry[0]);
		}
	}
	const makeURI = (entry) => {
		const defsSegment = ctx.target === "draft-2020-12" ? "$defs" : "definitions";
		if (ctx.external) {
			const externalId = ctx.external.registry.get(entry[0])?.id;
			const uriGenerator = ctx.external.uri ?? ((id) => id);
			if (externalId) return { ref: uriGenerator(externalId) };
			const id = entry[1].defId ?? entry[1].schema.id ?? `schema${ctx.counter++}`;
			entry[1].defId = id;
			return {
				defId: id,
				ref: `${uriGenerator("__shared")}#/${defsSegment}/${encodeJSONPointerSegment(id)}`
			};
		}
		const uriPrefix = `#`;
		const defUriPrefix = `${uriPrefix}/${defsSegment}/`;
		if (entry[1] === root && !entry[1].schema.id) return { ref: uriPrefix };
		const defId = entry[1].schema.id ?? `__schema${ctx.counter++}`;
		return {
			defId,
			ref: defUriPrefix + encodeJSONPointerSegment(defId)
		};
	};
	const extractToDef = (entry) => {
		if (entry[1].schema.$ref) return;
		const seen = entry[1];
		const { ref, defId } = makeURI(entry);
		seen.def = { ...seen.schema };
		if (defId) seen.defId = defId;
		const schema = seen.schema;
		for (const key in schema) delete schema[key];
		schema.$ref = ref;
	};
	if (ctx.cycles === "throw") for (const entry of ctx.seen.entries()) {
		const seen = entry[1];
		if (seen.cycle) throw new Error(`Cycle detected: #/${seen.cycle?.join("/")}/<root>

Set the \`cycles\` parameter to \`"ref"\` to resolve cyclical schemas with defs.`);
	}
	for (const entry of ctx.seen.entries()) {
		const seen = entry[1];
		if (schema === entry[0]) {
			extractToDef(entry);
			continue;
		}
		if (ctx.external) {
			const ext = ctx.external.registry.get(entry[0])?.id;
			if (schema !== entry[0] && ext) {
				extractToDef(entry);
				continue;
			}
		}
		if (ctx.metadataRegistry.get(entry[0])?.id) {
			extractToDef(entry);
			continue;
		}
		if (seen.cycle) {
			extractToDef(entry);
			continue;
		}
		if (seen.count > 1) {
			if (ctx.reused === "ref") extractToDef(entry);
		}
	}
	if (ctx.external) ctx.sharedDefsExtractedFor = ctx.external;
}
/** Rewrites `anyOf: [{type: "a"}, {type: "b"}]` to `type: ["a", "b"]`, which every JSON Schema draft treats as equivalent and most consumers render far better for the nullable case. Only branches that are a bare type assertion qualify — anything carrying a constraint, `$ref`, `const` or metadata is left alone. Runs after `flattenRef`, so a branch an override decorated or `$defs` extraction turned into a `$ref` is no longer bare and correctly stays in `anyOf`. `oneOf` is excluded: `integer` and `number` overlap, so "exactly one" and "at least one" are not the same there. OpenAPI 3.0 is excluded: its `type` must be a single string. */
function compactTypeUnion(schema) {
	const options = schema.anyOf;
	if (!Array.isArray(options) || options.length === 0 || schema.type !== void 0) return;
	const types = [];
	for (const option of options) {
		if (!option || typeof option !== "object") return;
		compactTypeUnion(option);
		const keys = Object.keys(option);
		if (keys.length !== 1 || keys[0] !== "type") return;
		const type = option.type;
		for (const member of Array.isArray(type) ? type : [type]) {
			if (typeof member !== "string") return;
			if (!types.includes(member)) types.push(member);
		}
	}
	delete schema.anyOf;
	schema.type = types.length === 1 ? types[0] : types;
}
/** Keywords `foldIntersection` knows how to combine. Anything else — `$ref`, `patternProperties`,
* an annotation like `description` — makes a member unfoldable, so a constraint this does not
* understand leaves the `allOf` alone instead of being silently dropped or misattributed. */
const FOLDABLE_KEYS = /* @__PURE__ */ new Set([
	"type",
	"properties",
	"required",
	"additionalProperties"
]);
const UNION_KEYS = ["oneOf", "anyOf"];
/** A member's constraint on a key it does not declare itself. A `catchall` states one; `false`, an absent `additionalProperties`, and the empty schema a loose object emits state nothing. */
function undeclaredConstraint(member) {
	const extra = member.additionalProperties;
	if (extra === void 0 || extra === false || typeof extra !== "object" || extra === null) return null;
	return Object.keys(extra).length ? extra : null;
}
/** Combines object members into the single object they describe together, or returns `null` if any of them carries a keyword outside {@link FOLDABLE_KEYS}. */
function foldObjects(members) {
	const objects = [];
	for (const member of members) {
		if (typeof member !== "object" || member.type !== "object") return null;
		for (const key in member) if (!FOLDABLE_KEYS.has(key)) return null;
		objects.push(member);
	}
	const properties = {};
	const required = /* @__PURE__ */ new Set();
	for (const object of objects) {
		for (const key in object.properties) {
			if (Object.prototype.hasOwnProperty.call(properties, key)) continue;
			const parts = [];
			for (const other of objects) {
				const part = other.properties?.[key] ?? undeclaredConstraint(other);
				if (part === null || part === void 0) continue;
				if (!parts.some((seen) => JSON.stringify(seen) === JSON.stringify(part))) parts.push(part);
			}
			assignProp(properties, key, parts.length === 1 ? parts[0] : foldObjects(parts) ?? { allOf: parts });
		}
		for (const key of object.required ?? []) required.add(key);
	}
	const folded = {
		type: "object",
		properties
	};
	if (required.size) folded.required = [...required];
	if (objects.every((object) => object.additionalProperties === false)) folded.additionalProperties = false;
	else {
		const constraints = [];
		for (const object of objects) {
			const constraint = undeclaredConstraint(object);
			if (constraint && !constraints.some((seen) => JSON.stringify(seen) === JSON.stringify(constraint))) constraints.push(constraint);
		}
		if (constraints.length === 1) folded.additionalProperties = constraints[0];
		else if (constraints.length > 1) folded.additionalProperties = { allOf: constraints };
	}
	return folded;
}
/** `additionalProperties` in an `allOf` member sees only that member's own `properties`, so two
* closed object members reject each other's keys and the schema validates nothing. Zod's parser
* pools the key sets instead — `handleIntersectionResults` reports a key as unrecognized only when
* *every* side rejects it — so the emitted schema has to pool them too, and folding the members
* into one object is the encoding that says so on every target.
*
* This runs from `finalize`, after `extractDefs`, which is what keeps it clear of the `$ref`
* machinery: a member extracted into `$defs` is already a `$ref` by now and declines to fold, so it
* keeps its reference and its own closedness rather than being inlined as a stale copy. */
function foldIntersection(json) {
	const allOf = json.allOf;
	if (!Array.isArray(allOf) || allOf.length < 2) return;
	for (const key of FOLDABLE_KEYS) if (key in json) return;
	const unions = allOf.filter((m) => UNION_KEYS.some((k) => Array.isArray(m[k])));
	let folded = null;
	if (!unions.length) folded = foldObjects(allOf);
	else {
		const union = unions[0];
		const keyword = UNION_KEYS.find((k) => Array.isArray(union[k]));
		if (Object.keys(union).length !== 1) return;
		const rest = allOf.filter((m) => m !== union);
		const branches = union[keyword].map((branch) => foldObjects([...rest, branch]));
		if (branches.some((b) => !b)) return;
		folded = { [keyword]: branches };
	}
	if (!folded) return;
	delete json.allOf;
	assignProps(json, folded);
}
function finalize(ctx, schema) {
	const root = ctx.seen.get(schema);
	if (!root) throw new Error("Unprocessed schema. This is a bug in Zod.");
	const flattenRef = (zodSchema) => {
		const seen = ctx.seen.get(zodSchema);
		if (seen.ref === null) return;
		const schema = seen.def ?? seen.schema;
		const _cached = { ...schema };
		const ref = seen.ref;
		seen.ref = null;
		if (ref) {
			flattenRef(ref);
			const refSeen = ctx.seen.get(ref);
			const refSchema = refSeen.schema;
			if (refSchema.$ref && (ctx.target === "draft-07" || ctx.target === "draft-04" || ctx.target === "openapi-3.0")) {
				schema.allOf = schema.allOf ?? [];
				schema.allOf.push(refSchema);
			} else assignProps(schema, refSchema);
			assignProps(schema, _cached);
			if (zodSchema._zod.parent === ref) for (const key in schema) {
				if (key === "$ref" || key === "allOf") continue;
				if (!(key in _cached)) delete schema[key];
			}
			if (refSchema.$ref && refSeen.def) for (const key in schema) {
				if (key === "$ref" || key === "allOf") continue;
				if (key in refSeen.def && JSON.stringify(schema[key]) === JSON.stringify(refSeen.def[key])) delete schema[key];
			}
		}
		const parent = zodSchema._zod.parent;
		if (parent && parent !== ref) {
			flattenRef(parent);
			const parentSeen = ctx.seen.get(parent);
			if (parentSeen?.schema.$ref) {
				schema.$ref = parentSeen.schema.$ref;
				if (parentSeen.def) for (const key in schema) {
					if (key === "$ref" || key === "allOf") continue;
					if (key in parentSeen.def && JSON.stringify(schema[key]) === JSON.stringify(parentSeen.def[key])) delete schema[key];
				}
			}
		}
		ctx.override({
			zodSchema,
			jsonSchema: schema,
			path: seen.path ?? []
		});
	};
	if (!ctx.external || ctx.sharedEmitDoneFor !== ctx.external) {
		for (const entry of [...ctx.seen.entries()].reverse()) flattenRef(entry[0]);
		if (ctx.target !== "openapi-3.0") for (const entry of ctx.seen.entries()) compactTypeUnion(entry[1].def ?? entry[1].schema);
		for (const rewrite of ctx.deferred) rewrite();
		if (ctx.intersections.length) {
			const carriers = /* @__PURE__ */ new Map();
			for (const seen of ctx.seen.values()) for (const json of [seen.schema, seen.def]) {
				const allOf = json?.allOf;
				if (!Array.isArray(allOf)) continue;
				const existing = carriers.get(allOf);
				if (existing) existing.push(json);
				else carriers.set(allOf, [json]);
			}
			for (const allOf of ctx.intersections) for (const json of carriers.get(allOf) ?? []) foldIntersection(json);
		}
	}
	const result = {};
	if (ctx.target === "draft-2020-12") result.$schema = "https://json-schema.org/draft/2020-12/schema";
	else if (ctx.target === "draft-07") result.$schema = "http://json-schema.org/draft-07/schema#";
	else if (ctx.target === "draft-04") result.$schema = "http://json-schema.org/draft-04/schema#";
	else if (ctx.target === "openapi-3.0") {}
	if (ctx.external?.uri) {
		const id = ctx.external.registry.get(schema)?.id;
		if (!id) throw new Error("Schema is missing an `id` property");
		result.$id = ctx.external.uri(id);
	}
	assignProps(result, root.defId ? root.schema : root.def ?? root.schema);
	const rootMetaId = ctx.metadataRegistry.get(schema)?.id;
	if (rootMetaId !== void 0 && result.id === rootMetaId) delete result.id;
	const defs = ctx.external?.defs ?? {};
	if (!ctx.external || ctx.sharedEmitDoneFor !== ctx.external) for (const entry of ctx.seen.entries()) {
		const seen = entry[1];
		if (seen.def && seen.defId) {
			if (seen.def.id === seen.defId) delete seen.def.id;
			assignProp(defs, seen.defId, seen.def);
		}
	}
	if (ctx.external) ctx.sharedEmitDoneFor = ctx.external;
	if (ctx.external) {} else if (Object.keys(defs).length > 0) {
		if (ctx.target === "draft-2020-12") result.$defs = defs;
		else result.definitions = defs;
	}
	try {
		const finalized = JSON.parse(JSON.stringify(result));
		Object.defineProperty(finalized, "~standard", {
			value: {
				...schema["~standard"],
				jsonSchema: {
					input: createStandardJSONSchemaMethod(schema, "input", ctx.processors),
					output: createStandardJSONSchemaMethod(schema, "output", ctx.processors)
				}
			},
			enumerable: false,
			writable: false
		});
		return finalized;
	} catch (_err) {
		throw new Error("Error converting schema to JSON.");
	}
}
function isTransforming(_schema, _ctx) {
	const ctx = _ctx ?? { seen: /* @__PURE__ */ new Set() };
	if (ctx.seen.has(_schema)) return false;
	ctx.seen.add(_schema);
	const def = _schema._zod.def;
	if (def.type === "transform") return true;
	if (def.type === "array") return isTransforming(def.element, ctx);
	if (def.type === "set") return isTransforming(def.valueType, ctx);
	if (def.type === "lazy") return isTransforming(def.getter(), ctx);
	if (def.type === "promise" || def.type === "optional" || def.type === "nonoptional" || def.type === "nullable" || def.type === "readonly" || def.type === "default" || def.type === "prefault" || def.type === "catch") return isTransforming(def.innerType, ctx);
	if (def.type === "intersection") return isTransforming(def.left, ctx) || isTransforming(def.right, ctx);
	if (def.type === "record" || def.type === "map") return isTransforming(def.keyType, ctx) || isTransforming(def.valueType, ctx);
	if (def.type === "pipe") {
		if (_schema._zod.traits.has("$ZodCodec")) return true;
		return isTransforming(def.in, ctx) || isTransforming(def.out, ctx);
	}
	if (def.type === "object") {
		for (const key in def.shape) if (isTransforming(def.shape[key], ctx)) return true;
		return false;
	}
	if (def.type === "union") {
		for (const option of def.options) if (isTransforming(option, ctx)) return true;
		return false;
	}
	if (def.type === "tuple") {
		for (const item of def.items) if (isTransforming(item, ctx)) return true;
		if (def.rest && isTransforming(def.rest, ctx)) return true;
		return false;
	}
	return false;
}
/**
* Creates a toJSONSchema method for a schema instance.
* This encapsulates the logic of initializing context, processing, extracting defs, and finalizing.
*/
const createToJSONSchemaMethod = (schema, processors = {}) => (params) => {
	const ctx = initializeContext({
		...params,
		processors
	});
	processSchema(schema, ctx);
	extractDefs(ctx, schema);
	return finalize(ctx, schema);
};
const createStandardJSONSchemaMethod = (schema, io, processors = {}) => (params) => {
	const { libraryOptions, target } = params ?? {};
	const ctx = initializeContext({
		...libraryOptions ?? {},
		target,
		io,
		processors
	});
	processSchema(schema, ctx);
	extractDefs(ctx, schema);
	return finalize(ctx, schema);
};
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/core/json-schema-processors.js
const narrowMin = (agg, key, value) => {
	if (agg[key] === void 0 || value > agg[key]) agg[key] = value;
};
const narrowMax = (agg, key, value) => {
	if (agg[key] === void 0 || value < agg[key]) agg[key] = value;
};
const narrowBoth = (agg, value) => {
	narrowMin(agg, "minimum", value);
	narrowMax(agg, "maximum", value);
};
const addDivisor = (agg, value) => {
	agg.multipleOf ?? (agg.multipleOf = []);
	if (!agg.multipleOf.includes(value)) agg.multipleOf.push(value);
};
const addPattern = (agg, pattern) => {
	agg.patterns ?? (agg.patterns = /* @__PURE__ */ new Set());
	agg.patterns.add(pattern);
};
const intersectMime = (agg, mime) => {
	agg.mime = agg.mime ? agg.mime.filter((m) => mime.includes(m)) : [...mime];
};
const setFormat = (agg, format) => {
	agg.format = format;
	if (format.includes("int")) agg.isInt = true;
};
const minContributor = (agg, def) => narrowMin(agg, "minimum", def.minimum);
const maxContributor = (agg, def) => narrowMax(agg, "maximum", def.maximum);
const formatContributor = (ranges) => (agg, def) => {
	setFormat(agg, def.format);
	const [minimum, maximum] = ranges[def.format];
	narrowMin(agg, "minimum", minimum);
	narrowMax(agg, "maximum", maximum);
};
const contributors = {
	greater_than: (agg, def) => narrowMin(agg, def.inclusive ? "minimum" : "exclusiveMinimum", def.value),
	less_than: (agg, def) => narrowMax(agg, def.inclusive ? "maximum" : "exclusiveMaximum", def.value),
	multiple_of: (agg, def) => addDivisor(agg, def.value),
	number_format: formatContributor(NUMBER_FORMAT_RANGES),
	bigint_format: formatContributor(BIGINT_FORMAT_RANGES),
	min_length: minContributor,
	max_length: maxContributor,
	length_equals: (agg, def) => narrowBoth(agg, def.length),
	min_size: minContributor,
	max_size: maxContributor,
	size_equals: (agg, def) => narrowBoth(agg, def.size),
	string_format: (agg, def) => {
		setFormat(agg, def.format);
		if (def.pattern) addPattern(agg, def.pattern);
		if (def.format === "base64" || def.format === "base64url") agg.contentEncoding = def.format;
		if (def.local || def.precision === -1) agg.laxFormat = true;
	},
	mime_type: (agg, def) => intersectMime(agg, def.mime)
};
function aggregateChecks(schema) {
	const agg = {};
	const def = schema._zod.def;
	const list = schema._zod.traits.has("$ZodCheck") ? [schema, ...def.checks ?? []] : def.checks ?? [];
	for (const ch of list) contributors[ch._zod.def.check]?.(agg, ch._zod.def);
	const bag = schema._zod.bag;
	if (bag.minimum !== void 0) narrowMin(agg, "minimum", bag.minimum);
	if (bag.exclusiveMinimum !== void 0) narrowMin(agg, "exclusiveMinimum", bag.exclusiveMinimum);
	if (bag.maximum !== void 0) narrowMax(agg, "maximum", bag.maximum);
	if (bag.exclusiveMaximum !== void 0) narrowMax(agg, "exclusiveMaximum", bag.exclusiveMaximum);
	if (bag.multipleOf !== void 0) addDivisor(agg, bag.multipleOf);
	if (bag.format !== void 0) {
		agg.format ?? (agg.format = bag.format);
		if (bag.format.includes("int")) agg.isInt = true;
	}
	if (bag.mime) intersectMime(agg, bag.mime);
	for (const pattern of bag.patterns ?? []) addPattern(agg, pattern);
	return agg;
}
const formatMap = {
	guid: "uuid",
	url: "uri",
	datetime: "date-time",
	json_string: "json-string",
	regex: ""
};
const exactPatterns = /* @__PURE__ */ new Map([[base64Charset, base64], [base64urlCharset, base64url]]);
const exactPattern = (p) => exactPatterns.get(p) ?? p;
const stringProcessor = (schema, ctx, _json, _params) => {
	const json = _json;
	json.type = "string";
	const { minimum, maximum, format, patterns, contentEncoding, laxFormat } = aggregateChecks(schema);
	if (typeof minimum === "number") json.minLength = minimum;
	if (typeof maximum === "number") json.maxLength = maximum;
	if (format) {
		json.format = formatMap[format] ?? format;
		if (json.format === "") delete json.format;
		if (format === "time" || laxFormat) delete json.format;
	}
	if (contentEncoding) json.contentEncoding = contentEncoding;
	if (patterns && patterns.size > 0) {
		const patternList = [...patterns].map(exactPattern);
		if (patternList.length === 1) json.pattern = patternList[0].source;
		else if (patternList.length > 1) json.allOf = [...patternList.map((regex) => ({
			...ctx.target === "draft-07" || ctx.target === "draft-04" || ctx.target === "openapi-3.0" ? { type: "string" } : {},
			pattern: regex.source
		}))];
	}
};
const numberProcessor = (schema, ctx, _json, params) => {
	const json = _json;
	const { minimum, maximum, multipleOf, exclusiveMaximum, exclusiveMinimum, isInt } = aggregateChecks(schema);
	json.type = isInt ? "integer" : "number";
	const exMin = typeof exclusiveMinimum === "number" && exclusiveMinimum >= (minimum ?? Number.NEGATIVE_INFINITY);
	const exMax = typeof exclusiveMaximum === "number" && exclusiveMaximum <= (maximum ?? Number.POSITIVE_INFINITY);
	const legacy = ctx.target === "draft-04" || ctx.target === "openapi-3.0";
	if (exMin) {
		if (legacy) {
			json.minimum = exclusiveMinimum;
			json.exclusiveMinimum = true;
		} else json.exclusiveMinimum = exclusiveMinimum;
	} else if (typeof minimum === "number") json.minimum = minimum;
	if (exMax) {
		if (legacy) {
			json.maximum = exclusiveMaximum;
			json.exclusiveMaximum = true;
		} else json.exclusiveMaximum = exclusiveMaximum;
	} else if (typeof maximum === "number") json.maximum = maximum;
	if (multipleOf) {
		const divisors = /* @__PURE__ */ new Set();
		for (const divisor of multipleOf) if (Number.isFinite(divisor) && divisor !== 0) divisors.add(Math.abs(divisor));
		else handleUnrepresentable(schema, ctx, json, params, `A multipleOf divisor of ${divisor} cannot be represented in JSON Schema`);
		const [first, ...rest] = divisors;
		if (first !== void 0) json.multipleOf = first;
		if (rest.length) json.allOf = [...json.allOf ?? [], ...rest.map((m) => ({ multipleOf: m }))];
	}
};
const booleanProcessor = (_schema, _ctx, json, _params) => {
	json.type = "boolean";
};
const nullProcessor = (_schema, ctx, json, _params) => {
	if (ctx.target === "openapi-3.0") {
		json.type = "string";
		json.nullable = true;
		json.enum = [null];
	} else json.type = "null";
};
const neverProcessor = (_schema, _ctx, json, _params) => {
	json.not = {};
};
const enumProcessor = (schema, _ctx, json, _params) => {
	const def = schema._zod.def;
	const values = getEnumValues(def.entries);
	if (values.length === 0) {
		json.not = {};
		return;
	}
	if (values.every((v) => typeof v === "number")) json.type = "number";
	if (values.every((v) => typeof v === "string")) json.type = "string";
	json.enum = values;
};
const literalProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	if (def.values.length === 0) {
		json.not = {};
		return;
	}
	const vals = [];
	for (const val of def.values) if (val === void 0) {
		if (handleUnrepresentable(schema, ctx, json, params, "Literal `undefined` cannot be represented in JSON Schema")) return;
	} else if (typeof val === "bigint") {
		if (handleUnrepresentable(schema, ctx, json, params, "BigInt literals cannot be represented in JSON Schema")) return;
		vals.push(Number(val));
	} else vals.push(val);
	if (vals.length === 0) {} else if (vals.length === 1) {
		const val = vals[0];
		json.type = val === null ? "null" : typeof val;
		if (ctx.target === "draft-04" || ctx.target === "openapi-3.0") json.enum = [val];
		else json.const = val;
	} else {
		if (vals.every((v) => typeof v === "number")) json.type = "number";
		if (vals.every((v) => typeof v === "string")) json.type = "string";
		if (vals.every((v) => typeof v === "boolean")) json.type = "boolean";
		if (vals.every((v) => v === null)) json.type = "null";
		json.enum = vals;
	}
};
const customProcessor = (schema, ctx, json, params) => {
	handleUnrepresentable(schema, ctx, json, params, "Custom types cannot be represented in JSON Schema");
};
const transformProcessor = (schema, ctx, json, params) => {
	handleUnrepresentable(schema, ctx, json, params, "Transforms cannot be represented in JSON Schema");
};
const arrayProcessor = (schema, ctx, _json, params) => {
	const json = _json;
	const def = schema._zod.def;
	const { minimum, maximum } = aggregateChecks(schema);
	if (typeof minimum === "number") json.minItems = minimum;
	if (typeof maximum === "number") json.maxItems = maximum;
	json.type = "array";
	json.items = processSchema(def.element, ctx, {
		...params,
		path: [...params.path, "items"]
	});
};
function inputOptin(schema) {
	const def = schema._zod.def;
	if (def.type === "pipe" && def.in._zod.traits.has("$ZodTransform")) return inputOptin(def.out);
	if (def.type === "catch") return inputOptin(def.innerType);
	return schema._zod.optin;
}
const objectProcessor = (schema, ctx, _json, params) => {
	const json = _json;
	const def = schema._zod.def;
	const shape = def.shape;
	if (Object.getOwnPropertySymbols(shape).length && handleUnrepresentable(schema, ctx, json, params, "Symbol keys cannot be represented in JSON Schema")) return;
	json.type = "object";
	json.properties = {};
	for (const key in shape) assignProp(json.properties, key, processSchema(shape[key], ctx, {
		...params,
		path: [
			...params.path,
			"properties",
			key
		]
	}));
	const requiredKeys = [];
	for (const key of Object.keys(shape)) {
		const field = def.shape[key];
		if (ctx.io === "input" ? inputOptin(field) === void 0 : field._zod.optout === void 0) requiredKeys.push(key);
	}
	if (requiredKeys.length > 0) json.required = requiredKeys;
	if (def.catchall?._zod.def.type === "never") json.additionalProperties = false;
	else if (!def.catchall) {
		if (ctx.io === "output") json.additionalProperties = false;
	} else if (def.catchall) json.additionalProperties = processSchema(def.catchall, ctx, {
		...params,
		path: [...params.path, "additionalProperties"]
	});
};
const unionProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	const isExclusive = def.inclusive === false;
	const options = def.options.map((x, i) => processSchema(x, ctx, {
		...params,
		path: [
			...params.path,
			isExclusive ? "oneOf" : "anyOf",
			i
		]
	}));
	if (isExclusive) json.oneOf = options;
	else json.anyOf = options;
};
const intersectionProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	const a = processSchema(def.left, ctx, {
		...params,
		path: [
			...params.path,
			"allOf",
			0
		]
	});
	const b = processSchema(def.right, ctx, {
		...params,
		path: [
			...params.path,
			"allOf",
			1
		]
	});
	const isSimpleIntersection = (val) => "allOf" in val && Object.keys(val).length === 1;
	const allOf = [...isSimpleIntersection(a) ? a.allOf : [a], ...isSimpleIntersection(b) ? b.allOf : [b]];
	json.allOf = allOf;
	ctx.intersections.push(allOf);
};
/** JSON object keys are always strings, so a numeric record key schema is re-expressed over the
* numeric-string form the record parser matches. Deferred to `finalize`, after the flatten: a key
* behind a wrapper only carries its own `type` before then, and a union key only has its branches.
*
* A numeric bound cannot apply to a property name, so `minimum` and its siblings are dropped rather
* than carried over: keeping them beside `type: "string"` reproduces the match-nothing schema this
* exists to fix. A key that carries one therefore emits wider than the record parses — `z.record(z.number().min(5), V)`
* accepts `"3"` — which is the deliberate trade, since throwing on it would reject an ordinary schema
* outright. */
function stringifyKeyNames(bySchema, json, visited) {
	if (json.$ref) {
		if (visited.has(json)) return json;
		visited.add(json);
		const def = bySchema.get(json)?.def;
		if (!def) return json;
		const inlined = stringifyKeyNames(bySchema, def, visited);
		return inlined === def ? json : inlined;
	}
	for (const keyword of ["anyOf", "oneOf"]) {
		const branches = json[keyword];
		if (!Array.isArray(branches)) continue;
		const mapped = branches.map((branch) => stringifyKeyNames(bySchema, branch, visited));
		if (mapped.some((branch, i) => branch !== branches[i])) json = {
			...json,
			[keyword]: mapped
		};
	}
	const types = Array.isArray(json.type) ? json.type : [json.type];
	const numericType = !types.includes("string") && types.some((t) => t === "number" || t === "integer");
	const values = json.enum ?? (json.const !== void 0 ? [json.const] : void 0);
	if (!numericType && !values?.some((v) => typeof v === "number")) return json;
	const { minimum, maximum, exclusiveMinimum, exclusiveMaximum, multipleOf, format, id, ...rest } = json;
	if (rest.enum) rest.enum = rest.enum.map((v) => typeof v === "number" ? String(v) : v);
	else if (typeof rest.const === "number") rest.const = String(rest.const);
	if (!numericType) return rest;
	rest.type = "string";
	if (!values) rest.pattern = (types.includes("number") ? number$2 : integer).source;
	return rest;
}
/** Every record of one conversion, so the carriers are found in a single pass rather than once per record. */
const pendingRecords = /* @__PURE__ */ new WeakMap();
function rewriteKeyNames(ctx) {
	const bySchema = /* @__PURE__ */ new Map();
	for (const entry of ctx.seen.values()) if (entry.def && !bySchema.has(entry.schema)) bySchema.set(entry.schema, entry);
	const rewrites = /* @__PURE__ */ new Map();
	for (const record of pendingRecords.get(ctx) ?? []) {
		const seen = ctx.seen.get(record);
		const names = (seen?.def ?? seen?.schema)?.propertyNames;
		if (!names || names === true || rewrites.has(names)) continue;
		const rewritten = stringifyKeyNames(bySchema, names, /* @__PURE__ */ new Set());
		if (rewritten !== names) rewrites.set(names, rewritten);
	}
	if (!rewrites.size) return;
	for (const entry of ctx.seen.values()) for (const carrier of [entry.schema, entry.def]) {
		const rewritten = carrier && rewrites.get(carrier.propertyNames);
		if (rewritten) carrier.propertyNames = rewritten;
	}
}
const recordProcessor = (schema, ctx, _json, params) => {
	const json = _json;
	const def = schema._zod.def;
	json.type = "object";
	const keyType = def.keyType;
	const patterns = aggregateChecks(keyType).patterns;
	if (def.mode === "loose" && patterns && patterns.size > 0) {
		const valueSchema = processSchema(def.valueType, ctx, {
			...params,
			path: [
				...params.path,
				"patternProperties",
				"*"
			]
		});
		json.patternProperties = {};
		for (const pattern of patterns) assignProp(json.patternProperties, exactPattern(pattern).source, valueSchema);
	} else {
		if (ctx.target === "draft-07" || ctx.target === "draft-2020-12") {
			json.propertyNames = processSchema(def.keyType, ctx, {
				...params,
				path: [...params.path, "propertyNames"]
			});
			let pending = pendingRecords.get(ctx);
			if (!pending) {
				pending = [];
				pendingRecords.set(ctx, pending);
				ctx.deferred.push(() => rewriteKeyNames(ctx));
			}
			pending.push(schema);
		}
		json.additionalProperties = processSchema(def.valueType, ctx, {
			...params,
			path: [...params.path, "additionalProperties"]
		});
	}
	const keyValues = keyType._zod.values;
	const omittableOnInput = ctx.io === "input" && inputOptin(def.valueType) !== void 0;
	if (keyValues && !def.partial && !omittableOnInput) {
		const validKeyValues = [...keyValues].filter((v) => typeof v === "string" || typeof v === "number");
		if (validKeyValues.length > 0) json.required = validKeyValues.map(String);
	}
};
const nullableProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	const inner = processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	if (ctx.target === "openapi-3.0") {
		seen.ref = def.innerType;
		json.nullable = true;
	} else json.anyOf = [inner, { type: "null" }];
};
const nonoptionalProcessor = (schema, ctx, _json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
};
/** Round-trips a default value through JSON so the emitted schema is guaranteed to be valid JSON.
* A BigInt has no reliable encoding, so it goes through `unrepresentable` like any other
* unrepresentable value. Returns a sentinel when the caller must not write a default of its own. */
const UNREPRESENTABLE_DEFAULT = Symbol();
function serializeDefaultValue(value, schema, ctx, json, params) {
	let unrepresentable = false;
	const serialized = JSON.stringify(value, (_, val) => {
		if (typeof val !== "bigint") return val;
		unrepresentable = true;
		return null;
	});
	if (!unrepresentable) return JSON.parse(serialized);
	handleUnrepresentable(schema, ctx, json, params, "BigInt defaults cannot be represented in JSON Schema");
	return UNREPRESENTABLE_DEFAULT;
}
const defaultProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
	const value = serializeDefaultValue(def.defaultValue, schema, ctx, json, params);
	if (value !== UNREPRESENTABLE_DEFAULT) json.default = value;
};
const prefaultProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
	if (ctx.io !== "input") return;
	const value = serializeDefaultValue(def.defaultValue, schema, ctx, json, params);
	if (value !== UNREPRESENTABLE_DEFAULT) json._prefault = value;
};
const catchProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
	let catchValue;
	try {
		catchValue = def.catchValue(void 0);
	} catch {
		handleUnrepresentable(schema, ctx, json, params, "Dynamic catch values are not supported in JSON Schema");
		return;
	}
	json.default = catchValue;
};
const pipeProcessor = (schema, ctx, _json, params) => {
	const def = schema._zod.def;
	const inIsTransform = def.in._zod.traits.has("$ZodTransform");
	const innerType = ctx.io === "input" ? inIsTransform ? def.out : def.in : def.out;
	processSchema(innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = innerType;
};
const readonlyProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
	json.readOnly = true;
};
const optionalProcessor = (schema, ctx, _json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
};
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/classic/errors.js
const _installedErrorProtos = /* @__PURE__ */ new WeakSet([Object.prototype, Error.prototype]);
function _lazyMethod(proto, key, make) {
	Object.defineProperty(proto, key, {
		configurable: true,
		enumerable: false,
		get() {
			const value = make(this);
			Object.defineProperty(this, key, {
				value,
				configurable: true,
				writable: true
			});
			return value;
		},
		set(value) {
			Object.defineProperty(this, key, {
				value,
				configurable: true,
				writable: true
			});
		}
	});
}
const initializer = (inst, issues) => {
	$ZodError.init(inst, issues);
	inst.name = "ZodError";
	const proto = Object.getPrototypeOf(inst);
	if (_installedErrorProtos.has(proto)) return;
	_installedErrorProtos.add(proto);
	_lazyMethod(proto, "format", (self) => (mapper) => formatError(self, mapper));
	_lazyMethod(proto, "flatten", (self) => (mapper) => flattenError(self, mapper));
	_lazyMethod(proto, "addIssue", (self) => (issue) => {
		self.issues.push(issue);
		self.message = JSON.stringify(self.issues, jsonStringifyReplacer, 2);
	});
	_lazyMethod(proto, "addIssues", (self) => (issues) => {
		self.issues.push(...issues);
		self.message = JSON.stringify(self.issues, jsonStringifyReplacer, 2);
	});
	Object.defineProperty(proto, "isEmpty", {
		configurable: true,
		enumerable: false,
		get() {
			return this.issues.length === 0;
		}
	});
};
const ZodRealError = /*@__PURE__*/ $constructor("ZodError", initializer, void 0, { Parent: Error });
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/classic/parse.js
const parse = /* @__PURE__ */ _parse(ZodRealError);
const parseAsync = /* @__PURE__ */ _parseAsync(ZodRealError);
const safeParse = /* @__PURE__ */ _safeParse(ZodRealError);
const safeParseAsync = /* @__PURE__ */ _safeParseAsync(ZodRealError);
const encode = /* @__PURE__ */ _encode(ZodRealError);
const decode = /* @__PURE__ */ _decode(ZodRealError);
const encodeAsync = /* @__PURE__ */ _encodeAsync(ZodRealError);
const decodeAsync = /* @__PURE__ */ _decodeAsync(ZodRealError);
const safeEncode = /* @__PURE__ */ _safeEncode(ZodRealError);
const safeDecode = /* @__PURE__ */ _safeDecode(ZodRealError);
const safeEncodeAsync = /* @__PURE__ */ _safeEncodeAsync(ZodRealError);
const safeDecodeAsync = /* @__PURE__ */ _safeDecodeAsync(ZodRealError);
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/classic/schemas.js
function _ensureDefaultLocale() {
	if (!globalConfig.localeError) config(en_default());
}
function _ensureDefaultMemoizer() {
	if (!globalConfig.memoizer) config({ memoizer: memoizer() });
}
const ZodType = /*@__PURE__*/ $constructor("ZodType", (inst, def) => {
	_ensureDefaultLocale();
	$ZodType.init(inst, def);
	inst.def = def;
	inst.type = def.type;
	return inst;
}, {
	check(...chks) {
		const def = this.def;
		return this.clone(mergeDefs(def, { checks: [...def.checks ?? [], ...chks.map((ch) => typeof ch === "function" ? { _zod: {
			check: ch,
			def: { check: "custom" },
			onattach: []
		} } : ch)] }), { parent: true });
	},
	with(...chks) {
		return this.check(...chks);
	},
	clone(def, params) {
		return clone(this, def, params);
	},
	brand() {
		return this;
	},
	register(reg, meta) {
		reg.add(this, meta);
		return this;
	},
	refine(check, params) {
		return this.check(refine(check, params));
	},
	superRefine(refinement, params) {
		return this.check(superRefine(refinement, params));
	},
	overwrite(fn) {
		return this.check(/* @__PURE__ */ _overwrite(fn));
	},
	optional() {
		return optional(this);
	},
	exactOptional() {
		return exactOptional(this);
	},
	nullable() {
		return nullable(this);
	},
	nullish() {
		return optional(nullable(this));
	},
	nonoptional(params) {
		return nonoptional(this, params);
	},
	array() {
		return array(this);
	},
	or(arg) {
		return union([this, arg]);
	},
	and(arg) {
		return intersection(this, arg);
	},
	transform(tx) {
		return pipe(this, transform(tx));
	},
	default(d) {
		return _default(this, d);
	},
	prefault(d) {
		return prefault(this, d);
	},
	catch(params) {
		return _catch(this, params);
	},
	pipe(target) {
		return pipe(this, target);
	},
	readonly() {
		return readonly(this);
	},
	describe(description) {
		const cl = this.clone();
		globalRegistry.add(cl, { description });
		return cl;
	},
	meta(...args) {
		if (args.length === 0) return globalRegistry.get(this);
		const cl = this.clone();
		globalRegistry.add(cl, args[0]);
		return cl;
	},
	isOptional() {
		return this.safeParse(void 0).success;
	},
	isNullable() {
		return this.safeParse(null).success;
	},
	apply(fn, ...args) {
		return args.length === 0 ? fn(this) : fn(this, ...args);
	},
	get "~standard"() {
		return hide(this, "~standard", {
			...standardProps(this),
			jsonSchema: {
				input: createStandardJSONSchemaMethod(this, "input"),
				output: createStandardJSONSchemaMethod(this, "output")
			}
		});
	},
	set "~standard"(value) {
		own(this, "~standard", value);
	},
	parse: function _parse(data, params) {
		return parse(this, data, params, { callee: _parse });
	},
	parseAsync: async function _parseAsync(data, params) {
		return await parseAsync(this, data, params, { callee: _parseAsync });
	},
	safeParse(data, params) {
		return safeParse(this, data, params);
	},
	async safeParseAsync(data, params) {
		return safeParseAsync(this, data, params);
	},
	get spa() {
		return this?.safeParseAsync;
	},
	set spa(value) {
		own(this, "spa", value);
	},
	validate(data, params) {
		return validate(this, data, params);
	},
	validateAsync(data, params) {
		return validateAsync$1(this, data, params);
	},
	encode: function _encode(data, params) {
		return encode(this, data, params, { callee: _encode });
	},
	decode: function _decode(data, params) {
		return decode(this, data, params, { callee: _decode });
	},
	encodeAsync: async function _encodeAsync(data, params) {
		return await encodeAsync(this, data, params, { callee: _encodeAsync });
	},
	decodeAsync: async function _decodeAsync(data, params) {
		return await decodeAsync(this, data, params, { callee: _decodeAsync });
	},
	safeEncode(data, params) {
		return safeEncode(this, data, params);
	},
	safeDecode(data, params) {
		return safeDecode(this, data, params);
	},
	async safeEncodeAsync(data, params) {
		return safeEncodeAsync(this, data, params);
	},
	async safeDecodeAsync(data, params) {
		return safeDecodeAsync(this, data, params);
	},
	toJSONSchema(params) {
		return createToJSONSchemaMethod(this, {})(params);
	},
	get description() {
		return globalRegistry.get(this)?.description;
	},
	get _def() {
		return this._zod.def;
	}
});
/** @internal */
const _ZodString = /*@__PURE__*/ $constructor("_ZodString", (inst, def) => {
	$ZodString.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => stringProcessor(inst, ctx, json, params);
}, /*@__PURE__*/ derived({
	format: (inst) => aggregateChecks(inst).format ?? null,
	minLength: (inst) => aggregateChecks(inst).minimum ?? null,
	maxLength: (inst) => aggregateChecks(inst).maximum ?? null
}, {
	regex(...args) {
		return this.check(/* @__PURE__ */ _regex(...args));
	},
	includes(...args) {
		return this.check(/* @__PURE__ */ _includes(...args));
	},
	startsWith(...args) {
		return this.check(/* @__PURE__ */ _startsWith(...args));
	},
	endsWith(...args) {
		return this.check(/* @__PURE__ */ _endsWith(...args));
	},
	min(...args) {
		return this.check(/* @__PURE__ */ _minLength(...args));
	},
	max(...args) {
		return this.check(/* @__PURE__ */ _maxLength(...args));
	},
	length(...args) {
		return this.check(/* @__PURE__ */ _length(...args));
	},
	nonempty(...args) {
		return this.check(/* @__PURE__ */ _minLength(1, ...args));
	},
	lowercase(params) {
		return this.check(/* @__PURE__ */ _lowercase(params));
	},
	uppercase(params) {
		return this.check(/* @__PURE__ */ _uppercase(params));
	},
	trim() {
		return this.check(/* @__PURE__ */ _trim());
	},
	normalize(...args) {
		return this.check(/* @__PURE__ */ _normalize(...args));
	},
	toLowerCase() {
		return this.check(/* @__PURE__ */ _toLowerCase());
	},
	toUpperCase() {
		return this.check(/* @__PURE__ */ _toUpperCase());
	},
	slugify() {
		return this.check(/* @__PURE__ */ _slugify());
	}
}));
const ZodString = /*@__PURE__*/ $constructor("ZodString", (inst, def) => {
	$ZodString.init(inst, def);
	_ZodString.init(inst, def);
}, {
	email(params) {
		return this.check(/* @__PURE__ */ _email(ZodEmail, params));
	},
	url(params) {
		return this.check(/* @__PURE__ */ _url(ZodURL, params));
	},
	jwt(params) {
		return this.check(/* @__PURE__ */ _jwt(ZodJWT, params));
	},
	emoji(params) {
		return this.check(/* @__PURE__ */ _emoji(ZodEmoji, params));
	},
	guid(params) {
		return this.check(/* @__PURE__ */ _guid(ZodGUID, params));
	},
	uuid(params) {
		return this.check(/* @__PURE__ */ _uuid(ZodUUID, params));
	},
	uuidv4(params) {
		return this.check(/* @__PURE__ */ _uuidv4(ZodUUID, params));
	},
	uuidv6(params) {
		return this.check(/* @__PURE__ */ _uuidv6(ZodUUID, params));
	},
	uuidv7(params) {
		return this.check(/* @__PURE__ */ _uuidv7(ZodUUID, params));
	},
	nanoid(params) {
		return this.check(/* @__PURE__ */ _nanoid(ZodNanoID, params));
	},
	cuid(params) {
		return this.check(/* @__PURE__ */ _cuid(ZodCUID, params));
	},
	cuid2(params) {
		return this.check(/* @__PURE__ */ _cuid2(ZodCUID2, params));
	},
	ulid(params) {
		return this.check(/* @__PURE__ */ _ulid(ZodULID, params));
	},
	base64(params) {
		return this.check(/* @__PURE__ */ _base64(ZodBase64, params));
	},
	base64url(params) {
		return this.check(/* @__PURE__ */ _base64url(ZodBase64URL, params));
	},
	xid(params) {
		return this.check(/* @__PURE__ */ _xid(ZodXID, params));
	},
	ksuid(params) {
		return this.check(/* @__PURE__ */ _ksuid(ZodKSUID, params));
	},
	ipv4(params) {
		return this.check(/* @__PURE__ */ _ipv4(ZodIPv4, params));
	},
	ipv6(params) {
		return this.check(/* @__PURE__ */ _ipv6(ZodIPv6, params));
	},
	cidrv4(params) {
		return this.check(/* @__PURE__ */ _cidrv4(ZodCIDRv4, params));
	},
	cidrv6(params) {
		return this.check(/* @__PURE__ */ _cidrv6(ZodCIDRv6, params));
	},
	e164(params) {
		return this.check(/* @__PURE__ */ _e164(ZodE164, params));
	},
	datetime(params) {
		return this.check(/* @__PURE__ */ _isoDateTime(ZodISODateTime, params));
	},
	date(params) {
		return this.check(/* @__PURE__ */ _isoDate(ZodISODate, params));
	},
	time(params) {
		return this.check(/* @__PURE__ */ _isoTime(ZodISOTime, params));
	},
	duration(params) {
		return this.check(/* @__PURE__ */ _isoDuration(ZodISODuration, params));
	}
});
function string(params) {
	return /* @__PURE__ */ _string(ZodString, params);
}
const ZodStringFormat = /*@__PURE__*/ $constructor("ZodStringFormat", (inst, def) => {
	$ZodStringFormat.init(inst, def);
	_ZodString.init(inst, def);
});
const ZodISODateTime = /*@__PURE__*/ $constructor("ZodISODateTime", (inst, def) => {
	$ZodISODateTime.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodISODate = /*@__PURE__*/ $constructor("ZodISODate", (inst, def) => {
	$ZodISODate.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodISOTime = /*@__PURE__*/ $constructor("ZodISOTime", (inst, def) => {
	$ZodISOTime.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodISODuration = /*@__PURE__*/ $constructor("ZodISODuration", (inst, def) => {
	$ZodISODuration.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodEmail = /*@__PURE__*/ $constructor("ZodEmail", (inst, def) => {
	$ZodEmail.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodGUID = /*@__PURE__*/ $constructor("ZodGUID", (inst, def) => {
	$ZodGUID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodUUID = /*@__PURE__*/ $constructor("ZodUUID", (inst, def) => {
	$ZodUUID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodURL = /*@__PURE__*/ $constructor("ZodURL", (inst, def) => {
	$ZodURL.init(inst, def);
	ZodStringFormat.init(inst, def);
});
function url(params) {
	return /* @__PURE__ */ _url(ZodURL, params);
}
const ZodEmoji = /*@__PURE__*/ $constructor("ZodEmoji", (inst, def) => {
	$ZodEmoji.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodNanoID = /*@__PURE__*/ $constructor("ZodNanoID", (inst, def) => {
	$ZodNanoID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
/**
* @deprecated CUID v1 is deprecated by its authors due to information leakage
* (timestamps embedded in the id). Use {@link ZodCUID2} instead.
* See https://github.com/paralleldrive/cuid.
*/
const ZodCUID = /*@__PURE__*/ $constructor("ZodCUID", (inst, def) => {
	$ZodCUID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodCUID2 = /*@__PURE__*/ $constructor("ZodCUID2", (inst, def) => {
	$ZodCUID2.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodULID = /*@__PURE__*/ $constructor("ZodULID", (inst, def) => {
	$ZodULID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodXID = /*@__PURE__*/ $constructor("ZodXID", (inst, def) => {
	$ZodXID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodKSUID = /*@__PURE__*/ $constructor("ZodKSUID", (inst, def) => {
	$ZodKSUID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodIPv4 = /*@__PURE__*/ $constructor("ZodIPv4", (inst, def) => {
	$ZodIPv4.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodIPv6 = /*@__PURE__*/ $constructor("ZodIPv6", (inst, def) => {
	$ZodIPv6.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodCIDRv4 = /*@__PURE__*/ $constructor("ZodCIDRv4", (inst, def) => {
	$ZodCIDRv4.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodCIDRv6 = /*@__PURE__*/ $constructor("ZodCIDRv6", (inst, def) => {
	$ZodCIDRv6.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodBase64 = /*@__PURE__*/ $constructor("ZodBase64", (inst, def) => {
	$ZodBase64.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodBase64URL = /*@__PURE__*/ $constructor("ZodBase64URL", (inst, def) => {
	$ZodBase64URL.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodE164 = /*@__PURE__*/ $constructor("ZodE164", (inst, def) => {
	$ZodE164.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodJWT = /*@__PURE__*/ $constructor("ZodJWT", (inst, def) => {
	$ZodJWT.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodNumber = /*@__PURE__*/ $constructor("ZodNumber", (inst, def) => {
	$ZodNumber.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => numberProcessor(inst, ctx, json, params);
	inst.isFinite = true;
}, /*@__PURE__*/ derived({
	minValue: (inst) => {
		const { minimum, exclusiveMinimum } = aggregateChecks(inst);
		return Math.max(minimum ?? Number.NEGATIVE_INFINITY, exclusiveMinimum ?? Number.NEGATIVE_INFINITY);
	},
	maxValue: (inst) => {
		const { maximum, exclusiveMaximum } = aggregateChecks(inst);
		return Math.min(maximum ?? Number.POSITIVE_INFINITY, exclusiveMaximum ?? Number.POSITIVE_INFINITY);
	},
	isInt: (inst) => {
		const { isInt, multipleOf } = aggregateChecks(inst);
		return !!isInt || !!multipleOf?.some(Number.isSafeInteger);
	},
	format: (inst) => aggregateChecks(inst).format ?? null
}, {
	gt(value, params) {
		return this.check(/* @__PURE__ */ _gt(value, params));
	},
	gte(value, params) {
		return this.check(/* @__PURE__ */ _gte(value, params));
	},
	min(value, params) {
		return this.check(/* @__PURE__ */ _gte(value, params));
	},
	lt(value, params) {
		return this.check(/* @__PURE__ */ _lt(value, params));
	},
	lte(value, params) {
		return this.check(/* @__PURE__ */ _lte(value, params));
	},
	max(value, params) {
		return this.check(/* @__PURE__ */ _lte(value, params));
	},
	int(params) {
		return this.check(int(params));
	},
	safe(params) {
		return this.check(int(params));
	},
	positive(params) {
		return this.check(/* @__PURE__ */ _gt(0, params));
	},
	nonnegative(params) {
		return this.check(/* @__PURE__ */ _gte(0, params));
	},
	negative(params) {
		return this.check(/* @__PURE__ */ _lt(0, params));
	},
	nonpositive(params) {
		return this.check(/* @__PURE__ */ _lte(0, params));
	},
	multipleOf(value, params) {
		return this.check(/* @__PURE__ */ _multipleOf(value, params));
	},
	step(value, params) {
		return this.check(/* @__PURE__ */ _multipleOf(value, params));
	},
	finite() {
		return this;
	}
}));
function number$1(params) {
	return /* @__PURE__ */ _number(ZodNumber, params);
}
const ZodNumberFormat = /*@__PURE__*/ $constructor("ZodNumberFormat", (inst, def) => {
	$ZodNumberFormat.init(inst, def);
	ZodNumber.init(inst, def);
});
function int(params) {
	return /* @__PURE__ */ _int(ZodNumberFormat, params);
}
const ZodBoolean = /*@__PURE__*/ $constructor("ZodBoolean", (inst, def) => {
	$ZodBoolean.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => booleanProcessor(inst, ctx, json, params);
});
function boolean(params) {
	return /* @__PURE__ */ _boolean(ZodBoolean, params);
}
const ZodNull = /*@__PURE__*/ $constructor("ZodNull", (inst, def) => {
	$ZodNull.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => nullProcessor(inst, ctx, json, params);
});
function _null(params) {
	return /* @__PURE__ */ _null$1(ZodNull, params);
}
const ZodAny = /*@__PURE__*/ $constructor("ZodAny", (inst, def) => {
	$ZodAny.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => void 0;
});
function any() {
	return /* @__PURE__ */ _any(ZodAny);
}
const ZodUnknown = /*@__PURE__*/ $constructor("ZodUnknown", (inst, def) => {
	$ZodUnknown.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => void 0;
});
function unknown() {
	return /* @__PURE__ */ _unknown(ZodUnknown);
}
const ZodNever = /*@__PURE__*/ $constructor("ZodNever", (inst, def) => {
	$ZodNever.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => neverProcessor(inst, ctx, json, params);
});
function never(params) {
	return /* @__PURE__ */ _never(ZodNever, params);
}
const ZodArray = /*@__PURE__*/ $constructor("ZodArray", (inst, def) => {
	_ensureDefaultMemoizer();
	$ZodArray.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => arrayProcessor(inst, ctx, json, params);
	inst.element = def.element;
}, {
	min(n, params) {
		return this.check(/* @__PURE__ */ _minLength(n, params));
	},
	nonempty(params) {
		return this.check(/* @__PURE__ */ _minLength(1, params));
	},
	max(n, params) {
		return this.check(/* @__PURE__ */ _maxLength(n, params));
	},
	length(n, params) {
		return this.check(/* @__PURE__ */ _length(n, params));
	},
	unwrap() {
		return this.element;
	}
});
function array(element, params) {
	return /* @__PURE__ */ _array(ZodArray, element, params);
}
const ZodObject = /*@__PURE__*/ $constructor("ZodObject", (inst, def) => {
	_ensureDefaultMemoizer();
	$ZodObjectJIT.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => objectProcessor(inst, ctx, json, params);
	installLazyProp(inst, "shape", (self) => self._zod.def.shape, false);
}, {
	keyof() {
		return _enum(Object.keys(this._zod.def.shape));
	},
	catchall(catchall) {
		return this.clone(mergeDefs(this._zod.def, { catchall }));
	},
	passthrough() {
		return this.clone(mergeDefs(this._zod.def, { catchall: unknown() }));
	},
	loose() {
		return this.clone(mergeDefs(this._zod.def, { catchall: unknown() }));
	},
	strict() {
		return this.clone(mergeDefs(this._zod.def, { catchall: never() }));
	},
	strip() {
		return this.clone(mergeDefs(this._zod.def, { catchall: void 0 }));
	},
	extend(incoming) {
		return extend(this, incoming);
	},
	safeExtend(incoming) {
		return safeExtend(this, incoming);
	},
	merge(other) {
		return merge(this, other);
	},
	pick(mask) {
		return pick(this, mask);
	},
	omit(mask) {
		return omit(this, mask);
	},
	partial(...args) {
		return partial(ZodOptional, this, args[0]);
	},
	exactPartial(...args) {
		return partial(ZodExactOptional, this, args[0], "exactPartial");
	},
	required(...args) {
		return required(ZodNonOptional, this, args[0]);
	}
});
function object(shape, params) {
	const def = {
		type: "object",
		shape: shape ?? {},
		...normalizeParams(params)
	};
	return new ZodObject(def);
}
function looseObject(shape, params) {
	return new ZodObject({
		type: "object",
		shape,
		catchall: unknown(),
		...normalizeParams(params)
	});
}
const ZodUnion = /*@__PURE__*/ $constructor("ZodUnion", (inst, def) => {
	$ZodUnion.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => unionProcessor(inst, ctx, json, params);
	inst.options = def.options;
});
function union(options, params) {
	return new ZodUnion({
		type: "union",
		options,
		...normalizeParams(params)
	});
}
const ZodDiscriminatedUnion = /*@__PURE__*/ $constructor("ZodDiscriminatedUnion", (inst, def) => {
	ZodUnion.init(inst, def);
	$ZodDiscriminatedUnion.init(inst, def);
});
function discriminatedUnion(discriminator, options, params) {
	return new ZodDiscriminatedUnion({
		type: "union",
		options,
		discriminator,
		...normalizeParams(params)
	});
}
const ZodIntersection = /*@__PURE__*/ $constructor("ZodIntersection", (inst, def) => {
	$ZodIntersection.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => intersectionProcessor(inst, ctx, json, params);
});
function intersection(left, right) {
	return new ZodIntersection({
		type: "intersection",
		left,
		right
	});
}
const ZodRecord = /*@__PURE__*/ $constructor("ZodRecord", (inst, def) => {
	_ensureDefaultMemoizer();
	$ZodRecord.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => recordProcessor(inst, ctx, json, params);
	inst.keyType = def.keyType;
	inst.valueType = def.valueType;
});
function record$1(keyType, valueType, params) {
	if (!valueType || !valueType._zod) return new ZodRecord({
		type: "record",
		keyType: string(),
		valueType: keyType,
		...normalizeParams(valueType)
	});
	return new ZodRecord({
		type: "record",
		keyType,
		valueType,
		...normalizeParams(params)
	});
}
const ZodEnum = /*@__PURE__*/ $constructor("ZodEnum", (inst, def) => {
	$ZodEnum.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => enumProcessor(inst, ctx, json, params);
	inst.enum = def.entries;
	inst.options = [...inst._zod.values];
	const keys = new Set(Object.keys(def.entries));
	inst.extract = (values, params) => {
		const newEntries = {};
		for (const value of values) if (keys.has(value)) newEntries[value] = def.entries[value];
		else throw new Error(`Key ${value} not found in enum`);
		return new ZodEnum({
			...def,
			checks: [],
			...normalizeParams(params),
			entries: newEntries
		});
	};
	inst.exclude = (values, params) => {
		const newEntries = { ...def.entries };
		for (const value of values) if (keys.has(value)) delete newEntries[value];
		else throw new Error(`Key ${value} not found in enum`);
		return new ZodEnum({
			...def,
			checks: [],
			...normalizeParams(params),
			entries: newEntries
		});
	};
});
function _enum(values, params) {
	const entries = Array.isArray(values) ? Object.fromEntries(values.map((v) => [v, v])) : values;
	return new ZodEnum({
		type: "enum",
		entries,
		...normalizeParams(params)
	});
}
const ZodLiteral = /*@__PURE__*/ $constructor("ZodLiteral", (inst, def) => {
	$ZodLiteral.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => literalProcessor(inst, ctx, json, params);
	inst.values = new Set(def.values);
	Object.defineProperty(inst, "value", { get() {
		if (def.values.length > 1) throw new Error("This schema contains multiple valid literal values. Use `.values` instead.");
		return def.values[0];
	} });
});
function literal(value, params) {
	return new ZodLiteral({
		type: "literal",
		values: Array.isArray(value) ? value : [value],
		...normalizeParams(params)
	});
}
const ZodTransform = /*@__PURE__*/ $constructor("ZodTransform", (inst, def) => {
	_ensureDefaultMemoizer();
	$ZodTransform.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => transformProcessor(inst, ctx, json, params);
	inst._zod.parse = (payload, _ctx) => {
		if (_ctx.direction === "backward") throw new $ZodEncodeError(inst.constructor.name);
		payload.addIssue = (issue$1) => {
			if (typeof issue$1 === "string") payload.issues.push(issue(issue$1, payload.value, def));
			else {
				const _issue = issue$1;
				if (_issue.fatal) _issue.continue = false;
				_issue.code ?? (_issue.code = "custom");
				if (!("input" in _issue)) _issue.input = payload.value;
				_issue.inst ?? (_issue.inst = inst);
				payload.issues.push(issue(_issue));
			}
		};
		const output = def.transform(payload.value, payload);
		if (output instanceof Promise) return output.then((output) => {
			payload.value = output;
			return payload;
		});
		payload.value = output;
		return payload;
	};
});
function transform(fn) {
	return new ZodTransform({
		type: "transform",
		transform: fn
	});
}
const ZodOptional = /*@__PURE__*/ $constructor("ZodOptional", (inst, def) => {
	$ZodOptional.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => optionalProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function optional(innerType) {
	return new ZodOptional({
		type: "optional",
		innerType
	});
}
const ZodExactOptional = /*@__PURE__*/ $constructor("ZodExactOptional", (inst, def) => {
	$ZodExactOptional.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => optionalProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function exactOptional(innerType) {
	return new ZodExactOptional({
		type: "optional",
		innerType
	});
}
const ZodNullable = /*@__PURE__*/ $constructor("ZodNullable", (inst, def) => {
	$ZodNullable.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => nullableProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function nullable(innerType) {
	return new ZodNullable({
		type: "nullable",
		innerType
	});
}
const ZodDefault = /*@__PURE__*/ $constructor("ZodDefault", (inst, def) => {
	$ZodDefault.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => defaultProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
	inst.removeDefault = inst.unwrap;
});
function _default(innerType, defaultValue) {
	return new ZodDefault({
		type: "default",
		innerType,
		get defaultValue() {
			return typeof defaultValue === "function" ? defaultValue() : shallowClone(defaultValue);
		}
	});
}
const ZodPrefault = /*@__PURE__*/ $constructor("ZodPrefault", (inst, def) => {
	$ZodPrefault.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => prefaultProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function prefault(innerType, defaultValue) {
	return new ZodPrefault({
		type: "prefault",
		innerType,
		get defaultValue() {
			return typeof defaultValue === "function" ? defaultValue() : shallowClone(defaultValue);
		}
	});
}
const ZodNonOptional = /*@__PURE__*/ $constructor("ZodNonOptional", (inst, def) => {
	$ZodNonOptional.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => nonoptionalProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function nonoptional(innerType, params) {
	return new ZodNonOptional({
		type: "nonoptional",
		innerType,
		...normalizeParams(params)
	});
}
const ZodCatch = /*@__PURE__*/ $constructor("ZodCatch", (inst, def) => {
	$ZodCatch.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => catchProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
	inst.removeCatch = inst.unwrap;
});
function _catch(innerType, catchValue) {
	return new ZodCatch({
		type: "catch",
		innerType,
		catchValue: typeof catchValue === "function" ? catchValue : constantCatch(catchValue)
	});
}
const ZodPipe = /*@__PURE__*/ $constructor("ZodPipe", (inst, def) => {
	$ZodPipe.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => pipeProcessor(inst, ctx, json, params);
	inst.in = def.in;
	inst.out = def.out;
});
function pipe(in_, out) {
	return new ZodPipe({
		type: "pipe",
		in: in_,
		out
	});
}
const ZodPreprocess = /*@__PURE__*/ $constructor("ZodPreprocess", (inst, def) => {
	ZodPipe.init(inst, def);
	$ZodPreprocess.init(inst, def);
});
const ZodReadonly = /*@__PURE__*/ $constructor("ZodReadonly", (inst, def) => {
	$ZodReadonly.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => readonlyProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function readonly(innerType) {
	return new ZodReadonly({
		type: "readonly",
		innerType
	});
}
const ZodCustom = /*@__PURE__*/ $constructor("ZodCustom", (inst, def) => {
	$ZodCustom.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => customProcessor(inst, ctx, json, params);
});
function custom(fn, _params) {
	return /* @__PURE__ */ _custom(ZodCustom, fn ?? (() => true), _params);
}
function refine(fn, _params = {}) {
	return /* @__PURE__ */ _refine(ZodCustom, fn, _params);
}
function superRefine(fn, params) {
	return /* @__PURE__ */ _superRefine(fn, params);
}
function preprocess(fn, schema) {
	return new ZodPreprocess({
		type: "pipe",
		in: transform(fn),
		out: schema
	});
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/classic/compat.js
/** @deprecated Use the raw string literal codes instead, e.g. "invalid_type". */
const ZodIssueCode = {
	invalid_type: "invalid_type",
	too_big: "too_big",
	too_small: "too_small",
	invalid_format: "invalid_format",
	not_multiple_of: "not_multiple_of",
	unrecognized_keys: "unrecognized_keys",
	invalid_union: "invalid_union",
	invalid_key: "invalid_key",
	invalid_element: "invalid_element",
	invalid_value: "invalid_value",
	custom: "custom"
};
/** @deprecated Do not use. Stub definition, only included for zod-to-json-schema compatibility. */
var ZodFirstPartyTypeKind;
ZodFirstPartyTypeKind || (ZodFirstPartyTypeKind = {});
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/classic/iso.js
function datetime(params) {
	return /* @__PURE__ */ _isoDateTime(ZodISODateTime, params);
}
// node_modules/.pnpm/zod@4.6.7/node_modules/zod/v4/classic/coerce.js
function number(params) {
	return /* @__PURE__ */ _coercedNumber(ZodNumber, params);
}
// node_modules/.pnpm/@modelcontextprotocol+sdk@1.32.2_zod@4.6.7/node_modules/@modelcontextprotocol/sdk/dist/esm/types.js
const LATEST_PROTOCOL_VERSION = "2025-11-25";
const RELATED_TASK_META_KEY = "io.modelcontextprotocol/related-task";
/**
* Assert 'object' type schema.
*
* @internal
*/
const AssertObjectSchema = custom((v) => v !== null && (typeof v === "object" || typeof v === "function"));
/**
* A progress token, used to associate progress notifications with the original request.
*/
const ProgressTokenSchema = union([string(), number$1().int()]);
/**
* An opaque token used to represent a cursor for pagination.
*/
const CursorSchema = string();
looseObject({
	/**
	* Requested duration in milliseconds to retain task from creation.
	*/
	ttl: number$1().optional(),
	/**
	* Time in milliseconds to wait between task status requests.
	*/
	pollInterval: number$1().optional()
});
const TaskMetadataSchema = object({ ttl: number$1().optional() });
/**
* Metadata for associating messages with a task.
* Include this in the `_meta` field under the key `io.modelcontextprotocol/related-task`.
*/
const RelatedTaskMetadataSchema = object({ taskId: string() });
const RequestMetaSchema = looseObject({
	/**
	* If specified, the caller is requesting out-of-band progress notifications for this request (as represented by notifications/progress). The value of this parameter is an opaque token that will be attached to any subsequent notifications. The receiver is not obligated to provide these notifications.
	*/
	progressToken: ProgressTokenSchema.optional(),
	/**
	* If specified, this request is related to the provided task.
	*/
	[RELATED_TASK_META_KEY]: RelatedTaskMetadataSchema.optional()
});
/**
* Common params for any request.
*/
const BaseRequestParamsSchema = object({ 
/**
* See [General fields: `_meta`](/specification/draft/basic/index#meta) for notes on `_meta` usage.
*/
_meta: RequestMetaSchema.optional() });
/**
* Common params for any task-augmented request.
*/
const TaskAugmentedRequestParamsSchema = BaseRequestParamsSchema.extend({ 
/**
* If specified, the caller is requesting task-augmented execution for this request.
* The request will return a CreateTaskResult immediately, and the actual result can be
* retrieved later via tasks/result.
*
* Task augmentation is subject to capability negotiation - receivers MUST declare support
* for task augmentation of specific request types in their capabilities.
*/
task: TaskMetadataSchema.optional() });
const RequestSchema = object({
	method: string(),
	params: BaseRequestParamsSchema.loose().optional()
});
const NotificationsParamsSchema = object({ 
/**
* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
* for notes on _meta usage.
*/
_meta: RequestMetaSchema.optional() });
const NotificationSchema = object({
	method: string(),
	params: NotificationsParamsSchema.loose().optional()
});
const ResultSchema = looseObject({ 
/**
* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
* for notes on _meta usage.
*/
_meta: RequestMetaSchema.optional() });
/**
* A uniquely identifying ID for a request in JSON-RPC.
*/
const RequestIdSchema = union([string(), number$1().int()]);
/**
* A request that expects a response.
*/
const JSONRPCRequestSchema = object({
	jsonrpc: literal("2.0"),
	id: RequestIdSchema,
	...RequestSchema.shape
}).strict();
/**
* A notification which does not expect a response.
*/
const JSONRPCNotificationSchema = object({
	jsonrpc: literal("2.0"),
	...NotificationSchema.shape
}).strict();
/**
* A successful (non-error) response to a request.
*/
const JSONRPCResultResponseSchema = object({
	jsonrpc: literal("2.0"),
	id: RequestIdSchema,
	result: ResultSchema
}).strict();
/**
* Error codes defined by the JSON-RPC specification.
*/
var ErrorCode;
(function(ErrorCode) {
	ErrorCode[ErrorCode["ConnectionClosed"] = -32e3] = "ConnectionClosed";
	ErrorCode[ErrorCode["RequestTimeout"] = -32001] = "RequestTimeout";
	ErrorCode[ErrorCode["ParseError"] = -32700] = "ParseError";
	ErrorCode[ErrorCode["InvalidRequest"] = -32600] = "InvalidRequest";
	ErrorCode[ErrorCode["MethodNotFound"] = -32601] = "MethodNotFound";
	ErrorCode[ErrorCode["InvalidParams"] = -32602] = "InvalidParams";
	ErrorCode[ErrorCode["InternalError"] = -32603] = "InternalError";
	ErrorCode[ErrorCode["UrlElicitationRequired"] = -32042] = "UrlElicitationRequired";
})(ErrorCode || (ErrorCode = {}));
/**
* A response to a request that indicates an error occurred.
*/
const JSONRPCErrorResponseSchema = object({
	jsonrpc: literal("2.0"),
	id: RequestIdSchema.optional(),
	error: object({
		/**
		* The error type that occurred.
		*/
		code: number$1().int(),
		/**
		* A short description of the error. The message SHOULD be limited to a concise single sentence.
		*/
		message: string(),
		/**
		* Additional information about the error. The value of this member is defined by the sender (e.g. detailed error information, nested errors etc.).
		*/
		data: unknown().optional()
	})
}).strict();
union([
	JSONRPCRequestSchema,
	JSONRPCNotificationSchema,
	JSONRPCResultResponseSchema,
	JSONRPCErrorResponseSchema
]);
union([JSONRPCResultResponseSchema, JSONRPCErrorResponseSchema]);
/**
* A response that indicates success but carries no data.
*/
const EmptyResultSchema = ResultSchema.strict();
const CancelledNotificationParamsSchema = NotificationsParamsSchema.extend({
	/**
	* The ID of the request to cancel.
	*
	* This MUST correspond to the ID of a request previously issued in the same direction.
	*/
	requestId: RequestIdSchema.optional(),
	/**
	* An optional string describing the reason for the cancellation. This MAY be logged or presented to the user.
	*/
	reason: string().optional()
});
/**
* This notification can be sent by either side to indicate that it is cancelling a previously-issued request.
*
* The request SHOULD still be in-flight, but due to communication latency, it is always possible that this notification MAY arrive after the request has already finished.
*
* This notification indicates that the result will be unused, so any associated processing SHOULD cease.
*
* A client MUST NOT attempt to cancel its `initialize` request.
*/
const CancelledNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/cancelled"),
	params: CancelledNotificationParamsSchema
});
/**
* Base schema to add `icons` property.
*
*/
const IconsSchema = object({ 
/**
* Optional set of sized icons that the client can display in a user interface.
*
* Clients that support rendering icons MUST support at least the following MIME types:
* - `image/png` - PNG images (safe, universal compatibility)
* - `image/jpeg` (and `image/jpg`) - JPEG images (safe, universal compatibility)
*
* Clients that support rendering icons SHOULD also support:
* - `image/svg+xml` - SVG images (scalable but requires security precautions)
* - `image/webp` - WebP images (modern, efficient format)
*/
icons: array(object({
	/**
	* URL or data URI for the icon.
	*/
	src: string(),
	/**
	* Optional MIME type for the icon.
	*/
	mimeType: string().optional(),
	/**
	* Optional array of strings that specify sizes at which the icon can be used.
	* Each string should be in WxH format (e.g., `"48x48"`, `"96x96"`) or `"any"` for scalable formats like SVG.
	*
	* If not provided, the client should assume that the icon can be used at any size.
	*/
	sizes: array(string()).optional(),
	/**
	* Optional specifier for the theme this icon is designed for. `light` indicates
	* the icon is designed to be used with a light background, and `dark` indicates
	* the icon is designed to be used with a dark background.
	*
	* If not provided, the client should assume the icon can be used with any theme.
	*/
	theme: _enum(["light", "dark"]).optional()
})).optional() });
/**
* Base metadata interface for common properties across resources, tools, prompts, and implementations.
*/
const BaseMetadataSchema = object({
	/** Intended for programmatic or logical use, but used as a display name in past specs or fallback */
	name: string(),
	/**
	* Intended for UI and end-user contexts — optimized to be human-readable and easily understood,
	* even by those unfamiliar with domain-specific terminology.
	*
	* If not provided, the name should be used for display (except for Tool,
	* where `annotations.title` should be given precedence over using `name`,
	* if present).
	*/
	title: string().optional()
});
/**
* Describes the name and version of an MCP implementation.
*/
const ImplementationSchema = BaseMetadataSchema.extend({
	...BaseMetadataSchema.shape,
	...IconsSchema.shape,
	version: string(),
	/**
	* An optional URL of the website for this implementation.
	*/
	websiteUrl: string().optional(),
	/**
	* An optional human-readable description of what this implementation does.
	*
	* This can be used by clients or servers to provide context about their purpose
	* and capabilities. For example, a server might describe the types of resources
	* or tools it provides, while a client might describe its intended use case.
	*/
	description: string().optional()
});
const ElicitationCapabilitySchema = preprocess((value) => {
	if (value && typeof value === "object" && !Array.isArray(value)) {
		if (Object.keys(value).length === 0) return { form: {} };
	}
	return value;
}, intersection(object({
	form: intersection(object({ applyDefaults: boolean().optional() }), record$1(string(), unknown())).optional(),
	url: AssertObjectSchema.optional()
}), record$1(string(), unknown()).optional()));
/**
* Task capabilities for clients, indicating which request types support task creation.
*/
const ClientTasksCapabilitySchema = looseObject({
	/**
	* Present if the client supports listing tasks.
	*/
	list: AssertObjectSchema.optional(),
	/**
	* Present if the client supports cancelling tasks.
	*/
	cancel: AssertObjectSchema.optional(),
	/**
	* Capabilities for task creation on specific request types.
	*/
	requests: looseObject({
		/**
		* Task support for sampling requests.
		*/
		sampling: looseObject({ createMessage: AssertObjectSchema.optional() }).optional(),
		/**
		* Task support for elicitation requests.
		*/
		elicitation: looseObject({ create: AssertObjectSchema.optional() }).optional()
	}).optional()
});
/**
* Task capabilities for servers, indicating which request types support task creation.
*/
const ServerTasksCapabilitySchema = looseObject({
	/**
	* Present if the server supports listing tasks.
	*/
	list: AssertObjectSchema.optional(),
	/**
	* Present if the server supports cancelling tasks.
	*/
	cancel: AssertObjectSchema.optional(),
	/**
	* Capabilities for task creation on specific request types.
	*/
	requests: looseObject({ 
	/**
	* Task support for tool requests.
	*/
tools: looseObject({ call: AssertObjectSchema.optional() }).optional() }).optional()
});
/**
* Capabilities a client may support. Known capabilities are defined here, in this schema, but this is not a closed set: any client can define its own, additional capabilities.
*/
const ClientCapabilitiesSchema = object({
	/**
	* Experimental, non-standard capabilities that the client supports.
	*/
	experimental: record$1(string(), AssertObjectSchema).optional(),
	/**
	* Present if the client supports sampling from an LLM.
	*/
	sampling: object({
		/**
		* Present if the client supports context inclusion via includeContext parameter.
		* If not declared, servers SHOULD only use `includeContext: "none"` (or omit it).
		*/
		context: AssertObjectSchema.optional(),
		/**
		* Present if the client supports tool use via tools and toolChoice parameters.
		*/
		tools: AssertObjectSchema.optional()
	}).optional(),
	/**
	* Present if the client supports eliciting user input.
	*/
	elicitation: ElicitationCapabilitySchema.optional(),
	/**
	* Present if the client supports listing roots.
	*/
	roots: object({ 
	/**
	* Whether the client supports issuing notifications for changes to the roots list.
	*/
listChanged: boolean().optional() }).optional(),
	/**
	* Present if the client supports task creation.
	*/
	tasks: ClientTasksCapabilitySchema.optional(),
	/**
	* Extensions that the client supports. Keys are extension identifiers (vendor-prefix/extension-name).
	*/
	extensions: record$1(string(), AssertObjectSchema).optional()
});
const InitializeRequestParamsSchema = BaseRequestParamsSchema.extend({
	/**
	* The latest version of the Model Context Protocol that the client supports. The client MAY decide to support older versions as well.
	*/
	protocolVersion: string(),
	capabilities: ClientCapabilitiesSchema,
	clientInfo: ImplementationSchema
});
/**
* This request is sent from the client to the server when it first connects, asking it to begin initialization.
*/
const InitializeRequestSchema = RequestSchema.extend({
	method: literal("initialize"),
	params: InitializeRequestParamsSchema
});
/**
* Capabilities that a server may support. Known capabilities are defined here, in this schema, but this is not a closed set: any server can define its own, additional capabilities.
*/
const ServerCapabilitiesSchema = object({
	/**
	* Experimental, non-standard capabilities that the server supports.
	*/
	experimental: record$1(string(), AssertObjectSchema).optional(),
	/**
	* Present if the server supports sending log messages to the client.
	*/
	logging: AssertObjectSchema.optional(),
	/**
	* Present if the server supports sending completions to the client.
	*/
	completions: AssertObjectSchema.optional(),
	/**
	* Present if the server offers any prompt templates.
	*/
	prompts: object({ 
	/**
	* Whether this server supports issuing notifications for changes to the prompt list.
	*/
listChanged: boolean().optional() }).optional(),
	/**
	* Present if the server offers any resources to read.
	*/
	resources: object({
		/**
		* Whether this server supports clients subscribing to resource updates.
		*/
		subscribe: boolean().optional(),
		/**
		* Whether this server supports issuing notifications for changes to the resource list.
		*/
		listChanged: boolean().optional()
	}).optional(),
	/**
	* Present if the server offers any tools to call.
	*/
	tools: object({ 
	/**
	* Whether this server supports issuing notifications for changes to the tool list.
	*/
listChanged: boolean().optional() }).optional(),
	/**
	* Present if the server supports task creation.
	*/
	tasks: ServerTasksCapabilitySchema.optional(),
	/**
	* Extensions that the server supports. Keys are extension identifiers (vendor-prefix/extension-name).
	*/
	extensions: record$1(string(), AssertObjectSchema).optional()
});
/**
* After receiving an initialize request from the client, the server sends this response.
*/
const InitializeResultSchema = ResultSchema.extend({
	/**
	* The version of the Model Context Protocol that the server wants to use. This may not match the version that the client requested. If the client cannot support this version, it MUST disconnect.
	*/
	protocolVersion: string(),
	capabilities: ServerCapabilitiesSchema,
	serverInfo: ImplementationSchema,
	/**
	* Instructions describing how to use the server and its features.
	*
	* This can be used by clients to improve the LLM's understanding of available tools, resources, etc. It can be thought of like a "hint" to the model. For example, this information MAY be added to the system prompt.
	*/
	instructions: string().optional()
});
/**
* This notification is sent from the client to the server after initialization has finished.
*/
const InitializedNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/initialized"),
	params: NotificationsParamsSchema.optional()
});
/**
* A ping, issued by either the server or the client, to check that the other party is still alive. The receiver must promptly respond, or else may be disconnected.
*/
const PingRequestSchema = RequestSchema.extend({
	method: literal("ping"),
	params: BaseRequestParamsSchema.optional()
});
const ProgressSchema = object({
	/**
	* The progress thus far. This should increase every time progress is made, even if the total is unknown.
	*/
	progress: number$1(),
	/**
	* Total number of items to process (or total progress required), if known.
	*/
	total: optional(number$1()),
	/**
	* An optional message describing the current progress.
	*/
	message: optional(string())
});
const ProgressNotificationParamsSchema = object({
	...NotificationsParamsSchema.shape,
	...ProgressSchema.shape,
	/**
	* The progress token which was given in the initial request, used to associate this notification with the request that is proceeding.
	*/
	progressToken: ProgressTokenSchema
});
/**
* An out-of-band notification used to inform the receiver of a progress update for a long-running request.
*
* @category notifications/progress
*/
const ProgressNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/progress"),
	params: ProgressNotificationParamsSchema
});
const PaginatedRequestParamsSchema = BaseRequestParamsSchema.extend({ 
/**
* An opaque token representing the current pagination position.
* If provided, the server should return results starting after this cursor.
*/
cursor: CursorSchema.optional() });
const PaginatedRequestSchema = RequestSchema.extend({ params: PaginatedRequestParamsSchema.optional() });
const PaginatedResultSchema = ResultSchema.extend({ 
/**
* An opaque token representing the pagination position after the last returned result.
* If present, there may be more results available.
*/
nextCursor: CursorSchema.optional() });
/**
* The status of a task.
* */
const TaskStatusSchema = _enum([
	"working",
	"input_required",
	"completed",
	"failed",
	"cancelled"
]);
/**
* A pollable state object associated with a request.
*/
const TaskSchema = object({
	taskId: string(),
	status: TaskStatusSchema,
	/**
	* Time in milliseconds to keep task results available after completion.
	* If null, the task has unlimited lifetime until manually cleaned up.
	*/
	ttl: union([number$1(), _null()]),
	/**
	* ISO 8601 timestamp when the task was created.
	*/
	createdAt: string(),
	/**
	* ISO 8601 timestamp when the task was last updated.
	*/
	lastUpdatedAt: string(),
	pollInterval: optional(number$1()),
	/**
	* Optional diagnostic message for failed tasks or other status information.
	*/
	statusMessage: optional(string())
});
/**
* Result returned when a task is created, containing the task data wrapped in a task field.
*/
const CreateTaskResultSchema = ResultSchema.extend({ task: TaskSchema });
/**
* Parameters for task status notification.
*/
const TaskStatusNotificationParamsSchema = NotificationsParamsSchema.merge(TaskSchema);
/**
* A notification sent when a task's status changes.
*/
const TaskStatusNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/tasks/status"),
	params: TaskStatusNotificationParamsSchema
});
/**
* A request to get the state of a specific task.
*/
const GetTaskRequestSchema = RequestSchema.extend({
	method: literal("tasks/get"),
	params: BaseRequestParamsSchema.extend({ taskId: string() })
});
/**
* The response to a tasks/get request.
*/
const GetTaskResultSchema = ResultSchema.merge(TaskSchema);
/**
* A request to get the result of a specific task.
*/
const GetTaskPayloadRequestSchema = RequestSchema.extend({
	method: literal("tasks/result"),
	params: BaseRequestParamsSchema.extend({ taskId: string() })
});
ResultSchema.loose();
/**
* A request to list tasks.
*/
const ListTasksRequestSchema = PaginatedRequestSchema.extend({ method: literal("tasks/list") });
/**
* The response to a tasks/list request.
*/
const ListTasksResultSchema = PaginatedResultSchema.extend({ tasks: array(TaskSchema) });
/**
* A request to cancel a specific task.
*/
const CancelTaskRequestSchema = RequestSchema.extend({
	method: literal("tasks/cancel"),
	params: BaseRequestParamsSchema.extend({ taskId: string() })
});
ResultSchema.merge(TaskSchema);
/**
* The contents of a specific resource or sub-resource.
*/
const ResourceContentsSchema = object({
	/**
	* The URI of this resource.
	*/
	uri: string(),
	/**
	* The MIME type of this resource, if known.
	*/
	mimeType: optional(string()),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
const TextResourceContentsSchema = ResourceContentsSchema.extend({ 
/**
* The text of the item. This must only be set if the item can actually be represented as text (not binary data).
*/
text: string() });
/**
* A Zod schema for validating Base64 strings that is more performant and
* robust for very large inputs than the default regex-based check. It avoids
* stack overflows by using the native `atob` function for validation.
*/
const Base64Schema = string().refine((val) => {
	try {
		atob(val);
		return true;
	} catch {
		return false;
	}
}, { message: "Invalid Base64 string" });
const BlobResourceContentsSchema = ResourceContentsSchema.extend({ 
/**
* A base64-encoded string representing the binary data of the item.
*/
blob: Base64Schema });
/**
* The sender or recipient of messages and data in a conversation.
*/
const RoleSchema = _enum(["user", "assistant"]);
/**
* Optional annotations providing clients additional context about a resource.
*/
const AnnotationsSchema = object({
	/**
	* Intended audience(s) for the resource.
	*/
	audience: array(RoleSchema).optional(),
	/**
	* Importance hint for the resource, from 0 (least) to 1 (most).
	*/
	priority: number$1().min(0).max(1).optional(),
	/**
	* ISO 8601 timestamp for the most recent modification.
	*/
	lastModified: datetime({ offset: true }).optional()
});
/**
* A known resource that the server is capable of reading.
*/
const ResourceSchema = object({
	...BaseMetadataSchema.shape,
	...IconsSchema.shape,
	/**
	* The URI of this resource.
	*/
	uri: string(),
	/**
	* A description of what this resource represents.
	*
	* This can be used by clients to improve the LLM's understanding of available resources. It can be thought of like a "hint" to the model.
	*/
	description: optional(string()),
	/**
	* The MIME type of this resource, if known.
	*/
	mimeType: optional(string()),
	/**
	* The size of the raw resource content, in bytes (i.e., before base64 encoding or any tokenization), if known.
	*
	* This can be used by Hosts to display file sizes and estimate context window usage.
	*/
	size: optional(number$1()),
	/**
	* Optional annotations for the client.
	*/
	annotations: AnnotationsSchema.optional(),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: optional(looseObject({}))
});
/**
* A template description for resources available on the server.
*/
const ResourceTemplateSchema = object({
	...BaseMetadataSchema.shape,
	...IconsSchema.shape,
	/**
	* A URI template (according to RFC 6570) that can be used to construct resource URIs.
	*/
	uriTemplate: string(),
	/**
	* A description of what this template is for.
	*
	* This can be used by clients to improve the LLM's understanding of available resources. It can be thought of like a "hint" to the model.
	*/
	description: optional(string()),
	/**
	* The MIME type for all resources that match this template. This should only be included if all resources matching this template have the same type.
	*/
	mimeType: optional(string()),
	/**
	* Optional annotations for the client.
	*/
	annotations: AnnotationsSchema.optional(),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: optional(looseObject({}))
});
/**
* Sent from the client to request a list of resources the server has.
*/
const ListResourcesRequestSchema = PaginatedRequestSchema.extend({ method: literal("resources/list") });
/**
* The server's response to a resources/list request from the client.
*/
const ListResourcesResultSchema = PaginatedResultSchema.extend({ resources: array(ResourceSchema) });
/**
* Sent from the client to request a list of resource templates the server has.
*/
const ListResourceTemplatesRequestSchema = PaginatedRequestSchema.extend({ method: literal("resources/templates/list") });
/**
* The server's response to a resources/templates/list request from the client.
*/
const ListResourceTemplatesResultSchema = PaginatedResultSchema.extend({ resourceTemplates: array(ResourceTemplateSchema) });
const ResourceRequestParamsSchema = BaseRequestParamsSchema.extend({ 
/**
* The URI of the resource to read. The URI can use any protocol; it is up to the server how to interpret it.
*
* @format uri
*/
uri: string() });
/**
* Parameters for a `resources/read` request.
*/
const ReadResourceRequestParamsSchema = ResourceRequestParamsSchema;
/**
* Sent from the client to the server, to read a specific resource URI.
*/
const ReadResourceRequestSchema = RequestSchema.extend({
	method: literal("resources/read"),
	params: ReadResourceRequestParamsSchema
});
/**
* The server's response to a resources/read request from the client.
*/
const ReadResourceResultSchema = ResultSchema.extend({ contents: array(union([TextResourceContentsSchema, BlobResourceContentsSchema])) });
/**
* An optional notification from the server to the client, informing it that the list of resources it can read from has changed. This may be issued by servers without any previous subscription from the client.
*/
const ResourceListChangedNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/resources/list_changed"),
	params: NotificationsParamsSchema.optional()
});
const SubscribeRequestParamsSchema = ResourceRequestParamsSchema;
/**
* Sent from the client to request resources/updated notifications from the server whenever a particular resource changes.
*/
const SubscribeRequestSchema = RequestSchema.extend({
	method: literal("resources/subscribe"),
	params: SubscribeRequestParamsSchema
});
const UnsubscribeRequestParamsSchema = ResourceRequestParamsSchema;
/**
* Sent from the client to request cancellation of resources/updated notifications from the server. This should follow a previous resources/subscribe request.
*/
const UnsubscribeRequestSchema = RequestSchema.extend({
	method: literal("resources/unsubscribe"),
	params: UnsubscribeRequestParamsSchema
});
/**
* Parameters for a `notifications/resources/updated` notification.
*/
const ResourceUpdatedNotificationParamsSchema = NotificationsParamsSchema.extend({ 
/**
* The URI of the resource that has been updated. This might be a sub-resource of the one that the client actually subscribed to.
*/
uri: string() });
/**
* A notification from the server to the client, informing it that a resource has changed and may need to be read again. This should only be sent if the client previously sent a resources/subscribe request.
*/
const ResourceUpdatedNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/resources/updated"),
	params: ResourceUpdatedNotificationParamsSchema
});
/**
* Describes an argument that a prompt can accept.
*/
const PromptArgumentSchema = object({
	/**
	* The name of the argument.
	*/
	name: string(),
	/**
	* A human-readable description of the argument.
	*/
	description: optional(string()),
	/**
	* Whether this argument must be provided.
	*/
	required: optional(boolean())
});
/**
* A prompt or prompt template that the server offers.
*/
const PromptSchema = object({
	...BaseMetadataSchema.shape,
	...IconsSchema.shape,
	/**
	* An optional description of what this prompt provides
	*/
	description: optional(string()),
	/**
	* A list of arguments to use for templating the prompt.
	*/
	arguments: optional(array(PromptArgumentSchema)),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: optional(looseObject({}))
});
/**
* Sent from the client to request a list of prompts and prompt templates the server has.
*/
const ListPromptsRequestSchema = PaginatedRequestSchema.extend({ method: literal("prompts/list") });
/**
* The server's response to a prompts/list request from the client.
*/
const ListPromptsResultSchema = PaginatedResultSchema.extend({ prompts: array(PromptSchema) });
/**
* Parameters for a `prompts/get` request.
*/
const GetPromptRequestParamsSchema = BaseRequestParamsSchema.extend({
	/**
	* The name of the prompt or prompt template.
	*/
	name: string(),
	/**
	* Arguments to use for templating the prompt.
	*/
	arguments: record$1(string(), string()).optional()
});
/**
* Used by the client to get a prompt provided by the server.
*/
const GetPromptRequestSchema = RequestSchema.extend({
	method: literal("prompts/get"),
	params: GetPromptRequestParamsSchema
});
/**
* Text provided to or from an LLM.
*/
const TextContentSchema = object({
	type: literal("text"),
	/**
	* The text content of the message.
	*/
	text: string(),
	/**
	* Optional annotations for the client.
	*/
	annotations: AnnotationsSchema.optional(),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
/**
* An image provided to or from an LLM.
*/
const ImageContentSchema = object({
	type: literal("image"),
	/**
	* The base64-encoded image data.
	*/
	data: Base64Schema,
	/**
	* The MIME type of the image. Different providers may support different image types.
	*/
	mimeType: string(),
	/**
	* Optional annotations for the client.
	*/
	annotations: AnnotationsSchema.optional(),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
/**
* An Audio provided to or from an LLM.
*/
const AudioContentSchema = object({
	type: literal("audio"),
	/**
	* The base64-encoded audio data.
	*/
	data: Base64Schema,
	/**
	* The MIME type of the audio. Different providers may support different audio types.
	*/
	mimeType: string(),
	/**
	* Optional annotations for the client.
	*/
	annotations: AnnotationsSchema.optional(),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
/**
* A tool call request from an assistant (LLM).
* Represents the assistant's request to use a tool.
*/
const ToolUseContentSchema = object({
	type: literal("tool_use"),
	/**
	* The name of the tool to invoke.
	* Must match a tool name from the request's tools array.
	*/
	name: string(),
	/**
	* Unique identifier for this tool call.
	* Used to correlate with ToolResultContent in subsequent messages.
	*/
	id: string(),
	/**
	* Arguments to pass to the tool.
	* Must conform to the tool's inputSchema.
	*/
	input: record$1(string(), unknown()),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
/**
* The contents of a resource, embedded into a prompt or tool call result.
*/
const EmbeddedResourceSchema = object({
	type: literal("resource"),
	resource: union([TextResourceContentsSchema, BlobResourceContentsSchema]),
	/**
	* Optional annotations for the client.
	*/
	annotations: AnnotationsSchema.optional(),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
/**
* A content block that can be used in prompts and tool results.
*/
const ContentBlockSchema = union([
	TextContentSchema,
	ImageContentSchema,
	AudioContentSchema,
	ResourceSchema.extend({ type: literal("resource_link") }),
	EmbeddedResourceSchema
]);
/**
* Describes a message returned as part of a prompt.
*/
const PromptMessageSchema = object({
	role: RoleSchema,
	content: ContentBlockSchema
});
/**
* The server's response to a prompts/get request from the client.
*/
const GetPromptResultSchema = ResultSchema.extend({
	/**
	* An optional description for the prompt.
	*/
	description: string().optional(),
	messages: array(PromptMessageSchema)
});
/**
* An optional notification from the server to the client, informing it that the list of prompts it offers has changed. This may be issued by servers without any previous subscription from the client.
*/
const PromptListChangedNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/prompts/list_changed"),
	params: NotificationsParamsSchema.optional()
});
/**
* Additional properties describing a Tool to clients.
*
* NOTE: all properties in ToolAnnotations are **hints**.
* They are not guaranteed to provide a faithful description of
* tool behavior (including descriptive properties like `title`).
*
* Clients should never make tool use decisions based on ToolAnnotations
* received from untrusted servers.
*/
const ToolAnnotationsSchema = object({
	/**
	* A human-readable title for the tool.
	*/
	title: string().optional(),
	/**
	* If true, the tool does not modify its environment.
	*
	* Default: false
	*/
	readOnlyHint: boolean().optional(),
	/**
	* If true, the tool may perform destructive updates to its environment.
	* If false, the tool performs only additive updates.
	*
	* (This property is meaningful only when `readOnlyHint == false`)
	*
	* Default: true
	*/
	destructiveHint: boolean().optional(),
	/**
	* If true, calling the tool repeatedly with the same arguments
	* will have no additional effect on the its environment.
	*
	* (This property is meaningful only when `readOnlyHint == false`)
	*
	* Default: false
	*/
	idempotentHint: boolean().optional(),
	/**
	* If true, this tool may interact with an "open world" of external
	* entities. If false, the tool's domain of interaction is closed.
	* For example, the world of a web search tool is open, whereas that
	* of a memory tool is not.
	*
	* Default: true
	*/
	openWorldHint: boolean().optional()
});
/**
* Execution-related properties for a tool.
*/
const ToolExecutionSchema = object({ 
/**
* Indicates the tool's preference for task-augmented execution.
* - "required": Clients MUST invoke the tool as a task
* - "optional": Clients MAY invoke the tool as a task or normal request
* - "forbidden": Clients MUST NOT attempt to invoke the tool as a task
*
* If not present, defaults to "forbidden".
*/
taskSupport: _enum([
	"required",
	"optional",
	"forbidden"
]).optional() });
/**
* Definition for a tool the client can call.
*/
const ToolSchema = object({
	...BaseMetadataSchema.shape,
	...IconsSchema.shape,
	/**
	* A human-readable description of the tool.
	*/
	description: string().optional(),
	/**
	* A JSON Schema 2020-12 object defining the expected parameters for the tool.
	* Must have type: 'object' at the root level per MCP spec.
	*/
	inputSchema: object({
		type: literal("object"),
		properties: record$1(string(), AssertObjectSchema).optional(),
		required: array(string()).optional()
	}).catchall(unknown()),
	/**
	* An optional JSON Schema 2020-12 object defining the structure of the tool's output
	* returned in the structuredContent field of a CallToolResult.
	* Must have type: 'object' at the root level per MCP spec.
	*/
	outputSchema: object({
		type: literal("object"),
		properties: record$1(string(), AssertObjectSchema).optional(),
		required: array(string()).optional()
	}).catchall(unknown()).optional(),
	/**
	* Optional additional tool information.
	*/
	annotations: ToolAnnotationsSchema.optional(),
	/**
	* Execution-related properties for this tool.
	*/
	execution: ToolExecutionSchema.optional(),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
/**
* Sent from the client to request a list of tools the server has.
*/
const ListToolsRequestSchema = PaginatedRequestSchema.extend({ method: literal("tools/list") });
/**
* The server's response to a tools/list request from the client.
*/
const ListToolsResultSchema = PaginatedResultSchema.extend({ tools: array(ToolSchema) });
/**
* The server's response to a tool call.
*/
const CallToolResultSchema = ResultSchema.extend({
	/**
	* A list of content objects that represent the result of the tool call.
	*
	* If the Tool does not define an outputSchema, this field MUST be present in the result.
	* For backwards compatibility, this field is always present, but it may be empty.
	*/
	content: array(ContentBlockSchema).default([]),
	/**
	* An object containing structured tool output.
	*
	* If the Tool defines an outputSchema, this field MUST be present in the result, and contain a JSON object that matches the schema.
	*/
	structuredContent: record$1(string(), unknown()).optional(),
	/**
	* Whether the tool call ended in an error.
	*
	* If not set, this is assumed to be false (the call was successful).
	*
	* Any errors that originate from the tool SHOULD be reported inside the result
	* object, with `isError` set to true, _not_ as an MCP protocol-level error
	* response. Otherwise, the LLM would not be able to see that an error occurred
	* and self-correct.
	*
	* However, any errors in _finding_ the tool, an error indicating that the
	* server does not support tool calls, or any other exceptional conditions,
	* should be reported as an MCP error response.
	*/
	isError: boolean().optional()
});
CallToolResultSchema.or(ResultSchema.extend({ toolResult: unknown() }));
/**
* Parameters for a `tools/call` request.
*/
const CallToolRequestParamsSchema = TaskAugmentedRequestParamsSchema.extend({
	/**
	* The name of the tool to call.
	*/
	name: string(),
	/**
	* Arguments to pass to the tool.
	*/
	arguments: record$1(string(), unknown()).optional()
});
/**
* Used by the client to invoke a tool provided by the server.
*/
const CallToolRequestSchema = RequestSchema.extend({
	method: literal("tools/call"),
	params: CallToolRequestParamsSchema
});
/**
* An optional notification from the server to the client, informing it that the list of tools it offers has changed. This may be issued by servers without any previous subscription from the client.
*/
const ToolListChangedNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/tools/list_changed"),
	params: NotificationsParamsSchema.optional()
});
object({
	/**
	* If true, the list will be refreshed automatically when a list changed notification is received.
	* The callback will be called with the updated list.
	*
	* If false, the callback will be called with null items, allowing manual refresh.
	*
	* @default true
	*/
	autoRefresh: boolean().default(true),
	/**
	* Debounce time in milliseconds for list changed notification processing.
	*
	* Multiple notifications received within this timeframe will only trigger one refresh.
	* Set to 0 to disable debouncing.
	*
	* @default 300
	*/
	debounceMs: number$1().int().nonnegative().default(300)
});
/**
* The severity of a log message.
*/
const LoggingLevelSchema = _enum([
	"debug",
	"info",
	"notice",
	"warning",
	"error",
	"critical",
	"alert",
	"emergency"
]);
/**
* Parameters for a `logging/setLevel` request.
*/
const SetLevelRequestParamsSchema = BaseRequestParamsSchema.extend({ 
/**
* The level of logging that the client wants to receive from the server. The server should send all logs at this level and higher (i.e., more severe) to the client as notifications/logging/message.
*/
level: LoggingLevelSchema });
/**
* A request from the client to the server, to enable or adjust logging.
*/
const SetLevelRequestSchema = RequestSchema.extend({
	method: literal("logging/setLevel"),
	params: SetLevelRequestParamsSchema
});
/**
* Parameters for a `notifications/message` notification.
*/
const LoggingMessageNotificationParamsSchema = NotificationsParamsSchema.extend({
	/**
	* The severity of this log message.
	*/
	level: LoggingLevelSchema,
	/**
	* An optional name of the logger issuing this message.
	*/
	logger: string().optional(),
	/**
	* The data to be logged, such as a string message or an object. Any JSON serializable type is allowed here.
	*/
	data: unknown()
});
/**
* Notification of a log message passed from server to client. If no logging/setLevel request has been sent from the client, the server MAY decide which messages to send automatically.
*/
const LoggingMessageNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/message"),
	params: LoggingMessageNotificationParamsSchema
});
/**
* The server's preferences for model selection, requested of the client during sampling.
*/
const ModelPreferencesSchema = object({
	/**
	* Optional hints to use for model selection.
	*/
	hints: array(object({ 
	/**
	* A hint for a model name.
	*/
name: string().optional() })).optional(),
	/**
	* How much to prioritize cost when selecting a model.
	*/
	costPriority: number$1().min(0).max(1).optional(),
	/**
	* How much to prioritize sampling speed (latency) when selecting a model.
	*/
	speedPriority: number$1().min(0).max(1).optional(),
	/**
	* How much to prioritize intelligence and capabilities when selecting a model.
	*/
	intelligencePriority: number$1().min(0).max(1).optional()
});
/**
* Controls tool usage behavior in sampling requests.
*/
const ToolChoiceSchema = object({ 
/**
* Controls when tools are used:
* - "auto": Model decides whether to use tools (default)
* - "required": Model MUST use at least one tool before completing
* - "none": Model MUST NOT use any tools
*/
mode: _enum([
	"auto",
	"required",
	"none"
]).optional() });
/**
* The result of a tool execution, provided by the user (server).
* Represents the outcome of invoking a tool requested via ToolUseContent.
*/
const ToolResultContentSchema = object({
	type: literal("tool_result"),
	toolUseId: string().describe("The unique identifier for the corresponding tool call."),
	content: array(ContentBlockSchema).default([]),
	structuredContent: object({}).loose().optional(),
	isError: boolean().optional(),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
/**
* Basic content types for sampling responses (without tool use).
* Used for backwards-compatible CreateMessageResult when tools are not used.
*/
const SamplingContentSchema = discriminatedUnion("type", [
	TextContentSchema,
	ImageContentSchema,
	AudioContentSchema
]);
/**
* Content block types allowed in sampling messages.
* This includes text, image, audio, tool use requests, and tool results.
*/
const SamplingMessageContentBlockSchema = discriminatedUnion("type", [
	TextContentSchema,
	ImageContentSchema,
	AudioContentSchema,
	ToolUseContentSchema,
	ToolResultContentSchema
]);
/**
* Describes a message issued to or received from an LLM API.
*/
const SamplingMessageSchema = object({
	role: RoleSchema,
	content: union([SamplingMessageContentBlockSchema, array(SamplingMessageContentBlockSchema)]),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
/**
* Parameters for a `sampling/createMessage` request.
*/
const CreateMessageRequestParamsSchema = TaskAugmentedRequestParamsSchema.extend({
	messages: array(SamplingMessageSchema),
	/**
	* The server's preferences for which model to select. The client MAY modify or omit this request.
	*/
	modelPreferences: ModelPreferencesSchema.optional(),
	/**
	* An optional system prompt the server wants to use for sampling. The client MAY modify or omit this prompt.
	*/
	systemPrompt: string().optional(),
	/**
	* A request to include context from one or more MCP servers (including the caller), to be attached to the prompt.
	* The client MAY ignore this request.
	*
	* Default is "none". Values "thisServer" and "allServers" are soft-deprecated. Servers SHOULD only use these values if the client
	* declares ClientCapabilities.sampling.context. These values may be removed in future spec releases.
	*/
	includeContext: _enum([
		"none",
		"thisServer",
		"allServers"
	]).optional(),
	temperature: number$1().optional(),
	/**
	* The requested maximum number of tokens to sample (to prevent runaway completions).
	*
	* The client MAY choose to sample fewer tokens than the requested maximum.
	*/
	maxTokens: number$1().int(),
	stopSequences: array(string()).optional(),
	/**
	* Optional metadata to pass through to the LLM provider. The format of this metadata is provider-specific.
	*/
	metadata: AssertObjectSchema.optional(),
	/**
	* Tools that the model may use during generation.
	* The client MUST return an error if this field is provided but ClientCapabilities.sampling.tools is not declared.
	*/
	tools: array(ToolSchema).optional(),
	/**
	* Controls how the model uses tools.
	* The client MUST return an error if this field is provided but ClientCapabilities.sampling.tools is not declared.
	* Default is `{ mode: "auto" }`.
	*/
	toolChoice: ToolChoiceSchema.optional()
});
/**
* A request from the server to sample an LLM via the client. The client has full discretion over which model to select. The client should also inform the user before beginning sampling, to allow them to inspect the request (human in the loop) and decide whether to approve it.
*/
const CreateMessageRequestSchema = RequestSchema.extend({
	method: literal("sampling/createMessage"),
	params: CreateMessageRequestParamsSchema
});
/**
* The client's response to a sampling/create_message request from the server.
* This is the backwards-compatible version that returns single content (no arrays).
* Used when the request does not include tools.
*/
const CreateMessageResultSchema = ResultSchema.extend({
	/**
	* The name of the model that generated the message.
	*/
	model: string(),
	/**
	* The reason why sampling stopped, if known.
	*
	* Standard values:
	* - "endTurn": Natural end of the assistant's turn
	* - "stopSequence": A stop sequence was encountered
	* - "maxTokens": Maximum token limit was reached
	*
	* This field is an open string to allow for provider-specific stop reasons.
	*/
	stopReason: optional(_enum([
		"endTurn",
		"stopSequence",
		"maxTokens"
	]).or(string())),
	role: RoleSchema,
	/**
	* Response content. Single content block (text, image, or audio).
	*/
	content: SamplingContentSchema
});
/**
* The client's response to a sampling/create_message request when tools were provided.
* This version supports array content for tool use flows.
*/
const CreateMessageResultWithToolsSchema = ResultSchema.extend({
	/**
	* The name of the model that generated the message.
	*/
	model: string(),
	/**
	* The reason why sampling stopped, if known.
	*
	* Standard values:
	* - "endTurn": Natural end of the assistant's turn
	* - "stopSequence": A stop sequence was encountered
	* - "maxTokens": Maximum token limit was reached
	* - "toolUse": The model wants to use one or more tools
	*
	* This field is an open string to allow for provider-specific stop reasons.
	*/
	stopReason: optional(_enum([
		"endTurn",
		"stopSequence",
		"maxTokens",
		"toolUse"
	]).or(string())),
	role: RoleSchema,
	/**
	* Response content. May be a single block or array. May include ToolUseContent if stopReason is "toolUse".
	*/
	content: union([SamplingMessageContentBlockSchema, array(SamplingMessageContentBlockSchema)])
});
/**
* Primitive schema definition for boolean fields.
*/
const BooleanSchemaSchema = object({
	type: literal("boolean"),
	title: string().optional(),
	description: string().optional(),
	default: boolean().optional()
});
/**
* Primitive schema definition for string fields.
*/
const StringSchemaSchema = object({
	type: literal("string"),
	title: string().optional(),
	description: string().optional(),
	minLength: number$1().optional(),
	maxLength: number$1().optional(),
	format: _enum([
		"email",
		"uri",
		"date",
		"date-time"
	]).optional(),
	default: string().optional()
});
/**
* Primitive schema definition for number fields.
*/
const NumberSchemaSchema = object({
	type: _enum(["number", "integer"]),
	title: string().optional(),
	description: string().optional(),
	minimum: number$1().optional(),
	maximum: number$1().optional(),
	default: number$1().optional()
});
/**
* Schema for single-selection enumeration without display titles for options.
*/
const UntitledSingleSelectEnumSchemaSchema = object({
	type: literal("string"),
	title: string().optional(),
	description: string().optional(),
	enum: array(string()),
	default: string().optional()
});
/**
* Schema for single-selection enumeration with display titles for each option.
*/
const TitledSingleSelectEnumSchemaSchema = object({
	type: literal("string"),
	title: string().optional(),
	description: string().optional(),
	oneOf: array(object({
		const: string(),
		title: string()
	})),
	default: string().optional()
});
/**
* Union of all primitive schema definitions.
*/
const PrimitiveSchemaDefinitionSchema = union([
	union([
		object({
			type: literal("string"),
			title: string().optional(),
			description: string().optional(),
			enum: array(string()),
			enumNames: array(string()).optional(),
			default: string().optional()
		}),
		union([UntitledSingleSelectEnumSchemaSchema, TitledSingleSelectEnumSchemaSchema]),
		union([object({
			type: literal("array"),
			title: string().optional(),
			description: string().optional(),
			minItems: number$1().optional(),
			maxItems: number$1().optional(),
			items: object({
				type: literal("string"),
				enum: array(string())
			}),
			default: array(string()).optional()
		}), object({
			type: literal("array"),
			title: string().optional(),
			description: string().optional(),
			minItems: number$1().optional(),
			maxItems: number$1().optional(),
			items: object({ anyOf: array(object({
				const: string(),
				title: string()
			})) }),
			default: array(string()).optional()
		})])
	]),
	BooleanSchemaSchema,
	StringSchemaSchema,
	NumberSchemaSchema
]);
/**
* The parameters for a request to elicit additional information from the user via the client.
*/
const ElicitRequestParamsSchema = union([TaskAugmentedRequestParamsSchema.extend({
	/**
	* The elicitation mode.
	*
	* Optional for backward compatibility. Clients MUST treat missing mode as "form".
	*/
	mode: literal("form").optional(),
	/**
	* The message to present to the user describing what information is being requested.
	*/
	message: string(),
	/**
	* A restricted subset of JSON Schema.
	* Only top-level properties are allowed, without nesting.
	*/
	requestedSchema: object({
		type: literal("object"),
		properties: record$1(string(), PrimitiveSchemaDefinitionSchema),
		required: array(string()).optional()
	})
}), TaskAugmentedRequestParamsSchema.extend({
	/**
	* The elicitation mode.
	*/
	mode: literal("url"),
	/**
	* The message to present to the user explaining why the interaction is needed.
	*/
	message: string(),
	/**
	* The ID of the elicitation, which must be unique within the context of the server.
	* The client MUST treat this ID as an opaque value.
	*/
	elicitationId: string(),
	/**
	* The URL that the user should navigate to.
	*/
	url: string().url()
})]);
/**
* A request from the server to elicit user input via the client.
* The client should present the message and form fields to the user (form mode)
* or navigate to a URL (URL mode).
*/
const ElicitRequestSchema = RequestSchema.extend({
	method: literal("elicitation/create"),
	params: ElicitRequestParamsSchema
});
/**
* Parameters for a `notifications/elicitation/complete` notification.
*
* @category notifications/elicitation/complete
*/
const ElicitationCompleteNotificationParamsSchema = NotificationsParamsSchema.extend({ 
/**
* The ID of the elicitation that completed.
*/
elicitationId: string() });
/**
* A notification from the server to the client, informing it of a completion of an out-of-band elicitation request.
*
* @category notifications/elicitation/complete
*/
const ElicitationCompleteNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/elicitation/complete"),
	params: ElicitationCompleteNotificationParamsSchema
});
/**
* The client's response to an elicitation/create request from the server.
*/
const ElicitResultSchema = ResultSchema.extend({
	/**
	* The user action in response to the elicitation.
	* - "accept": User submitted the form/confirmed the action
	* - "decline": User explicitly decline the action
	* - "cancel": User dismissed without making an explicit choice
	*/
	action: _enum([
		"accept",
		"decline",
		"cancel"
	]),
	/**
	* The submitted form data, only present when action is "accept".
	* Contains values matching the requested schema.
	* Per MCP spec, content is "typically omitted" for decline/cancel actions.
	* We normalize null to undefined for leniency while maintaining type compatibility.
	*/
	content: preprocess((val) => val === null ? void 0 : val, record$1(string(), union([
		string(),
		number$1(),
		boolean(),
		array(string())
	])).optional())
});
/**
* A reference to a resource or resource template definition.
*/
const ResourceTemplateReferenceSchema = object({
	type: literal("ref/resource"),
	/**
	* The URI or URI template of the resource.
	*/
	uri: string()
});
/**
* Identifies a prompt.
*/
const PromptReferenceSchema = object({
	type: literal("ref/prompt"),
	/**
	* The name of the prompt or prompt template
	*/
	name: string()
});
/**
* Parameters for a `completion/complete` request.
*/
const CompleteRequestParamsSchema = BaseRequestParamsSchema.extend({
	ref: union([PromptReferenceSchema, ResourceTemplateReferenceSchema]),
	/**
	* The argument's information
	*/
	argument: object({
		/**
		* The name of the argument
		*/
		name: string(),
		/**
		* The value of the argument to use for completion matching.
		*/
		value: string()
	}),
	context: object({ 
	/**
	* Previously-resolved variables in a URI template or prompt.
	*/
arguments: record$1(string(), string()).optional() }).optional()
});
/**
* A request from the client to the server, to ask for completion options.
*/
const CompleteRequestSchema = RequestSchema.extend({
	method: literal("completion/complete"),
	params: CompleteRequestParamsSchema
});
/**
* The server's response to a completion/complete request
*/
const CompleteResultSchema = ResultSchema.extend({ completion: looseObject({
	/**
	* An array of completion values. Must not exceed 100 items.
	*/
	values: array(string()).max(100),
	/**
	* The total number of completion options available. This can exceed the number of values actually sent in the response.
	*/
	total: optional(number$1().int()),
	/**
	* Indicates whether there are additional completion options beyond those provided in the current response, even if the exact total is unknown.
	*/
	hasMore: optional(boolean())
}) });
/**
* Represents a root directory or file that the server can operate on.
*/
const RootSchema = object({
	/**
	* The URI identifying the root. This *must* start with file:// for now.
	*/
	uri: string().startsWith("file://"),
	/**
	* An optional name for the root.
	*/
	name: string().optional(),
	/**
	* See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
	* for notes on _meta usage.
	*/
	_meta: record$1(string(), unknown()).optional()
});
/**
* Sent from the server to request a list of root URIs from the client.
*/
const ListRootsRequestSchema = RequestSchema.extend({
	method: literal("roots/list"),
	params: BaseRequestParamsSchema.optional()
});
/**
* The client's response to a roots/list request from the server.
*/
const ListRootsResultSchema = ResultSchema.extend({ roots: array(RootSchema) });
/**
* A notification from the client to the server, informing it that the list of roots has changed.
*/
const RootsListChangedNotificationSchema = NotificationSchema.extend({
	method: literal("notifications/roots/list_changed"),
	params: NotificationsParamsSchema.optional()
});
union([
	PingRequestSchema,
	InitializeRequestSchema,
	CompleteRequestSchema,
	SetLevelRequestSchema,
	GetPromptRequestSchema,
	ListPromptsRequestSchema,
	ListResourcesRequestSchema,
	ListResourceTemplatesRequestSchema,
	ReadResourceRequestSchema,
	SubscribeRequestSchema,
	UnsubscribeRequestSchema,
	CallToolRequestSchema,
	ListToolsRequestSchema,
	GetTaskRequestSchema,
	GetTaskPayloadRequestSchema,
	ListTasksRequestSchema,
	CancelTaskRequestSchema
]);
union([
	CancelledNotificationSchema,
	ProgressNotificationSchema,
	InitializedNotificationSchema,
	RootsListChangedNotificationSchema,
	TaskStatusNotificationSchema
]);
union([
	EmptyResultSchema,
	CreateMessageResultSchema,
	CreateMessageResultWithToolsSchema,
	ElicitResultSchema,
	ListRootsResultSchema,
	GetTaskResultSchema,
	ListTasksResultSchema,
	CreateTaskResultSchema
]);
union([
	PingRequestSchema,
	CreateMessageRequestSchema,
	ElicitRequestSchema,
	ListRootsRequestSchema,
	GetTaskRequestSchema,
	GetTaskPayloadRequestSchema,
	ListTasksRequestSchema,
	CancelTaskRequestSchema
]);
union([
	CancelledNotificationSchema,
	ProgressNotificationSchema,
	LoggingMessageNotificationSchema,
	ResourceUpdatedNotificationSchema,
	ResourceListChangedNotificationSchema,
	ToolListChangedNotificationSchema,
	PromptListChangedNotificationSchema,
	TaskStatusNotificationSchema,
	ElicitationCompleteNotificationSchema
]);
union([
	EmptyResultSchema,
	InitializeResultSchema,
	CompleteResultSchema,
	GetPromptResultSchema,
	ListPromptsResultSchema,
	ListResourcesResultSchema,
	ListResourceTemplatesResultSchema,
	ReadResourceResultSchema,
	CallToolResultSchema,
	ListToolsResultSchema,
	GetTaskResultSchema,
	ListTasksResultSchema,
	CreateTaskResultSchema
]);
// node_modules/.pnpm/@modelcontextprotocol+sdk@1.32.2_zod@4.6.7/node_modules/@modelcontextprotocol/sdk/dist/esm/shared/auth.js
/**
* Reusable URL validation that disallows javascript: scheme
*/
const SafeUrlSchema = url().superRefine((val, ctx) => {
	if (!URL.canParse(val)) {
		ctx.addIssue({
			code: ZodIssueCode.custom,
			message: "URL must be parseable",
			fatal: true
		});
		return NEVER;
	}
}).refine((url) => {
	const u = new URL(url);
	return u.protocol !== "javascript:" && u.protocol !== "data:" && u.protocol !== "vbscript:";
}, { message: "URL cannot use javascript:, data:, or vbscript: scheme" });
/**
* RFC 9728 OAuth Protected Resource Metadata
*/
const OAuthProtectedResourceMetadataSchema = looseObject({
	resource: string().url(),
	authorization_servers: array(SafeUrlSchema).optional(),
	jwks_uri: string().url().optional(),
	scopes_supported: array(string()).optional(),
	bearer_methods_supported: array(string()).optional(),
	resource_signing_alg_values_supported: array(string()).optional(),
	resource_name: string().optional(),
	resource_documentation: string().optional(),
	resource_policy_uri: string().url().optional(),
	resource_tos_uri: string().url().optional(),
	tls_client_certificate_bound_access_tokens: boolean().optional(),
	authorization_details_types_supported: array(string()).optional(),
	dpop_signing_alg_values_supported: array(string()).optional(),
	dpop_bound_access_tokens_required: boolean().optional()
});
/**
* RFC 8414 OAuth 2.0 Authorization Server Metadata
*/
const OAuthMetadataSchema = looseObject({
	issuer: string(),
	authorization_endpoint: SafeUrlSchema,
	token_endpoint: SafeUrlSchema,
	registration_endpoint: SafeUrlSchema.optional(),
	scopes_supported: array(string()).optional(),
	response_types_supported: array(string()),
	response_modes_supported: array(string()).optional(),
	grant_types_supported: array(string()).optional(),
	token_endpoint_auth_methods_supported: array(string()).optional(),
	token_endpoint_auth_signing_alg_values_supported: array(string()).optional(),
	service_documentation: SafeUrlSchema.optional(),
	revocation_endpoint: SafeUrlSchema.optional(),
	revocation_endpoint_auth_methods_supported: array(string()).optional(),
	revocation_endpoint_auth_signing_alg_values_supported: array(string()).optional(),
	introspection_endpoint: string().optional(),
	introspection_endpoint_auth_methods_supported: array(string()).optional(),
	introspection_endpoint_auth_signing_alg_values_supported: array(string()).optional(),
	code_challenge_methods_supported: array(string()).optional(),
	client_id_metadata_document_supported: boolean().optional()
});
/**
* OpenID Connect Discovery metadata that may include OAuth 2.0 fields
* This schema represents the real-world scenario where OIDC providers
* return a mix of OpenID Connect and OAuth 2.0 metadata fields
*/
const OpenIdProviderDiscoveryMetadataSchema = object({
	...looseObject({
		issuer: string(),
		authorization_endpoint: SafeUrlSchema,
		token_endpoint: SafeUrlSchema,
		userinfo_endpoint: SafeUrlSchema.optional(),
		jwks_uri: SafeUrlSchema,
		registration_endpoint: SafeUrlSchema.optional(),
		scopes_supported: array(string()).optional(),
		response_types_supported: array(string()),
		response_modes_supported: array(string()).optional(),
		grant_types_supported: array(string()).optional(),
		acr_values_supported: array(string()).optional(),
		subject_types_supported: array(string()),
		id_token_signing_alg_values_supported: array(string()),
		id_token_encryption_alg_values_supported: array(string()).optional(),
		id_token_encryption_enc_values_supported: array(string()).optional(),
		userinfo_signing_alg_values_supported: array(string()).optional(),
		userinfo_encryption_alg_values_supported: array(string()).optional(),
		userinfo_encryption_enc_values_supported: array(string()).optional(),
		request_object_signing_alg_values_supported: array(string()).optional(),
		request_object_encryption_alg_values_supported: array(string()).optional(),
		request_object_encryption_enc_values_supported: array(string()).optional(),
		token_endpoint_auth_methods_supported: array(string()).optional(),
		token_endpoint_auth_signing_alg_values_supported: array(string()).optional(),
		display_values_supported: array(string()).optional(),
		claim_types_supported: array(string()).optional(),
		claims_supported: array(string()).optional(),
		service_documentation: string().optional(),
		claims_locales_supported: array(string()).optional(),
		ui_locales_supported: array(string()).optional(),
		claims_parameter_supported: boolean().optional(),
		request_parameter_supported: boolean().optional(),
		request_uri_parameter_supported: boolean().optional(),
		require_request_uri_registration: boolean().optional(),
		op_policy_uri: SafeUrlSchema.optional(),
		op_tos_uri: SafeUrlSchema.optional(),
		client_id_metadata_document_supported: boolean().optional()
	}).shape,
	...OAuthMetadataSchema.pick({ code_challenge_methods_supported: true }).shape
});
/**
* OAuth 2.1 token response
*/
const OAuthTokensSchema = object({
	access_token: string(),
	id_token: string().optional(),
	token_type: string(),
	expires_in: number().optional(),
	scope: string().optional(),
	refresh_token: string().optional(),
	/**
	* Not part of the wire format: the authorization server this value was obtained from, added by
	* the client's `auth()` before it is stored and compared when it is read back.
	*/
	issuer: string().optional().catch(void 0)
}).strip();
/**
* OAuth 2.1 error response
*/
const OAuthErrorResponseSchema = object({
	error: string(),
	error_description: string().optional(),
	error_uri: string().optional()
});
/**
* Optional version of SafeUrlSchema that allows empty string for retrocompatibility on tos_uri and logo_uri
*/
const OptionalSafeUrlSchema = SafeUrlSchema.optional().or(literal("").transform(() => void 0));
/**
* RFC 7591 OAuth 2.0 Dynamic Client Registration metadata
*/
const OAuthClientMetadataSchema = object({
	redirect_uris: array(SafeUrlSchema),
	token_endpoint_auth_method: string().optional(),
	grant_types: array(string()).optional(),
	response_types: array(string()).optional(),
	client_name: string().optional(),
	client_uri: SafeUrlSchema.optional(),
	logo_uri: OptionalSafeUrlSchema,
	scope: string().optional(),
	contacts: array(string()).optional(),
	tos_uri: OptionalSafeUrlSchema,
	policy_uri: string().optional(),
	jwks_uri: SafeUrlSchema.optional(),
	jwks: any().optional(),
	software_id: string().optional(),
	software_version: string().optional(),
	software_statement: string().optional()
}).strip();
/**
* RFC 7591 OAuth 2.0 Dynamic Client Registration client information
*/
const OAuthClientInformationSchema = object({
	client_id: string(),
	client_secret: string().optional(),
	client_id_issued_at: number$1().optional(),
	client_secret_expires_at: number$1().optional(),
	/**
	* Not part of the wire format: the authorization server this value was obtained from, added by
	* the client's `auth()` before it is stored and compared when it is read back.
	*/
	issuer: string().optional().catch(void 0)
}).strip();
/**
* RFC 7591 OAuth 2.0 Dynamic Client Registration full response (client information plus metadata)
*/
const OAuthClientInformationFullSchema = OAuthClientMetadataSchema.merge(OAuthClientInformationSchema);
object({
	error: string(),
	error_description: string().optional()
}).strip();
object({
	token: string(),
	token_type_hint: string().optional()
}).strip();
// node_modules/.pnpm/@modelcontextprotocol+sdk@1.32.2_zod@4.6.7/node_modules/@modelcontextprotocol/sdk/dist/esm/shared/auth-utils.js
/**
* Utilities for handling OAuth resource URIs.
*/
/**
* Converts a server URL to a resource URL by removing the fragment.
* RFC 8707 section 2 states that resource URIs "MUST NOT include a fragment component".
* Keeps everything else unchanged (scheme, domain, port, path, query).
*/
function resourceUrlFromServerUrl(url) {
	const resourceURL = typeof url === "string" ? new URL(url) : new URL(url.href);
	resourceURL.hash = "";
	return resourceURL;
}
/**
* Checks if a requested resource URL matches a configured resource URL.
* A requested resource matches if it has the same scheme, domain, port,
* and its path starts with the configured resource's path.
*
* @param requestedResource The resource URL being requested
* @param configuredResource The resource URL that has been configured
* @returns true if the requested resource matches the configured resource, false otherwise
*/
function checkResourceAllowed({ requestedResource, configuredResource }) {
	const requested = typeof requestedResource === "string" ? new URL(requestedResource) : new URL(requestedResource.href);
	const configured = typeof configuredResource === "string" ? new URL(configuredResource) : new URL(configuredResource.href);
	if (requested.origin !== configured.origin) return false;
	if (requested.pathname.length < configured.pathname.length) return false;
	const requestedPath = requested.pathname.endsWith("/") ? requested.pathname : requested.pathname + "/";
	const configuredPath = configured.pathname.endsWith("/") ? configured.pathname : configured.pathname + "/";
	return requestedPath.startsWith(configuredPath);
}
// node_modules/.pnpm/@modelcontextprotocol+sdk@1.32.2_zod@4.6.7/node_modules/@modelcontextprotocol/sdk/dist/esm/server/auth/errors.js
/**
* Base class for all OAuth errors
*/
var OAuthError = class extends Error {
	constructor(message, errorUri) {
		super(message);
		this.errorUri = errorUri;
		this.name = this.constructor.name;
	}
	/**
	* Converts the error to a standard OAuth error response object
	*/
	toResponseObject() {
		const response = {
			error: this.errorCode,
			error_description: this.message
		};
		if (this.errorUri) response.error_uri = this.errorUri;
		return response;
	}
	get errorCode() {
		return this.constructor.errorCode;
	}
};
/**
* Invalid request error - The request is missing a required parameter,
* includes an invalid parameter value, includes a parameter more than once,
* or is otherwise malformed.
*/
var InvalidRequestError = class extends OAuthError {};
InvalidRequestError.errorCode = "invalid_request";
/**
* Invalid client error - Client authentication failed (e.g., unknown client, no client
* authentication included, or unsupported authentication method).
*/
var InvalidClientError = class extends OAuthError {};
InvalidClientError.errorCode = "invalid_client";
/**
* Invalid grant error - The provided authorization grant or refresh token is
* invalid, expired, revoked, does not match the redirection URI used in the
* authorization request, or was issued to another client.
*/
var InvalidGrantError = class extends OAuthError {};
InvalidGrantError.errorCode = "invalid_grant";
/**
* Unauthorized client error - The authenticated client is not authorized to use
* this authorization grant type.
*/
var UnauthorizedClientError = class extends OAuthError {};
UnauthorizedClientError.errorCode = "unauthorized_client";
/**
* Unsupported grant type error - The authorization grant type is not supported
* by the authorization server.
*/
var UnsupportedGrantTypeError = class extends OAuthError {};
UnsupportedGrantTypeError.errorCode = "unsupported_grant_type";
/**
* Invalid scope error - The requested scope is invalid, unknown, malformed, or
* exceeds the scope granted by the resource owner.
*/
var InvalidScopeError = class extends OAuthError {};
InvalidScopeError.errorCode = "invalid_scope";
/**
* Access denied error - The resource owner or authorization server denied the request.
*/
var AccessDeniedError = class extends OAuthError {};
AccessDeniedError.errorCode = "access_denied";
/**
* Server error - The authorization server encountered an unexpected condition
* that prevented it from fulfilling the request.
*/
var ServerError = class extends OAuthError {};
ServerError.errorCode = "server_error";
/**
* Temporarily unavailable error - The authorization server is currently unable to
* handle the request due to a temporary overloading or maintenance of the server.
*/
var TemporarilyUnavailableError = class extends OAuthError {};
TemporarilyUnavailableError.errorCode = "temporarily_unavailable";
/**
* Unsupported response type error - The authorization server does not support
* obtaining an authorization code using this method.
*/
var UnsupportedResponseTypeError = class extends OAuthError {};
UnsupportedResponseTypeError.errorCode = "unsupported_response_type";
/**
* Unsupported token type error - The authorization server does not support
* the requested token type.
*/
var UnsupportedTokenTypeError = class extends OAuthError {};
UnsupportedTokenTypeError.errorCode = "unsupported_token_type";
/**
* Invalid token error - The access token provided is expired, revoked, malformed,
* or invalid for other reasons.
*/
var InvalidTokenError = class extends OAuthError {};
InvalidTokenError.errorCode = "invalid_token";
/**
* Method not allowed error - The HTTP method used is not allowed for this endpoint.
* (Custom, non-standard error)
*/
var MethodNotAllowedError = class extends OAuthError {};
MethodNotAllowedError.errorCode = "method_not_allowed";
/**
* Too many requests error - Rate limit exceeded.
* (Custom, non-standard error based on RFC 6585)
*/
var TooManyRequestsError = class extends OAuthError {};
TooManyRequestsError.errorCode = "too_many_requests";
/**
* Invalid client metadata error - The client metadata is invalid.
* (Custom error for dynamic client registration - RFC 7591)
*/
var InvalidClientMetadataError = class extends OAuthError {};
InvalidClientMetadataError.errorCode = "invalid_client_metadata";
/**
* Insufficient scope error - The request requires higher privileges than provided by the access token.
*/
var InsufficientScopeError = class extends OAuthError {};
InsufficientScopeError.errorCode = "insufficient_scope";
/**
* Invalid target error - The requested resource is invalid, missing, unknown, or malformed.
* (Custom error for resource indicators - RFC 8707)
*/
var InvalidTargetError = class extends OAuthError {};
InvalidTargetError.errorCode = "invalid_target";
/**
* A full list of all OAuthErrors, enabling parsing from error responses
*/
const OAUTH_ERRORS = {
	[InvalidRequestError.errorCode]: InvalidRequestError,
	[InvalidClientError.errorCode]: InvalidClientError,
	[InvalidGrantError.errorCode]: InvalidGrantError,
	[UnauthorizedClientError.errorCode]: UnauthorizedClientError,
	[UnsupportedGrantTypeError.errorCode]: UnsupportedGrantTypeError,
	[InvalidScopeError.errorCode]: InvalidScopeError,
	[AccessDeniedError.errorCode]: AccessDeniedError,
	[ServerError.errorCode]: ServerError,
	[TemporarilyUnavailableError.errorCode]: TemporarilyUnavailableError,
	[UnsupportedResponseTypeError.errorCode]: UnsupportedResponseTypeError,
	[UnsupportedTokenTypeError.errorCode]: UnsupportedTokenTypeError,
	[InvalidTokenError.errorCode]: InvalidTokenError,
	[MethodNotAllowedError.errorCode]: MethodNotAllowedError,
	[TooManyRequestsError.errorCode]: TooManyRequestsError,
	[InvalidClientMetadataError.errorCode]: InvalidClientMetadataError,
	[InsufficientScopeError.errorCode]: InsufficientScopeError,
	[InvalidTargetError.errorCode]: InvalidTargetError
};
// node_modules/.pnpm/@modelcontextprotocol+sdk@1.32.2_zod@4.6.7/node_modules/@modelcontextprotocol/sdk/dist/esm/shared/transport.js
const MAX_REDIRECTS = 5;
const REDIRECT_STATUSES = [
	301,
	302,
	303,
	307,
	308
];
/** @internal Whether `to` has the scheme, host and port of `from`, or is its https form with both on default ports. */
function isWithinOrigin(from, to) {
	if (from.protocol === to.protocol && from.hostname === to.hostname && from.port === to.port) return true;
	return from.hostname === to.hostname && from.protocol === "http:" && from.port === "" && to.protocol === "https:" && to.port === "";
}
function redirectTarget(response, requestUrl) {
	const location = REDIRECT_STATUSES.includes(response.status) ? response.headers.get("location") : null;
	try {
		return location ? new URL(location, requestUrl) : void 0;
	} catch {
		return;
	}
}
/** @internal Follows the redirect in `response` while it stays within the origin of `url`; undefined when `response` is to be used as is. */
function followWithinOrigin(baseFetch, url, init, response, followed = 0) {
	const target = redirectTarget(response, url);
	const keepsMethod = response.status === 307 || response.status === 308 || (init?.method ?? "GET").toUpperCase() === "GET";
	if (!target || followed === MAX_REDIRECTS || !keepsMethod) return;
	const from = new URL(url);
	if ((target.username || target.password) && (target.username !== from.username || target.password !== from.password) || !isWithinOrigin(from, target)) return;
	return Promise.resolve(response.body?.cancel()).then(() => baseFetch(target, {
		...init,
		redirect: "manual"
	})).then((next) => followWithinOrigin(baseFetch, target, init, next, followed + 1) ?? next);
}
const followingFetches = /* @__PURE__ */ new WeakSet();
/** @internal Wraps a fetch so it follows a redirect only within the request's origin; any other redirect response is returned as is. */
function fetchWithinOrigin(baseFetch = fetch) {
	if (followingFetches.has(baseFetch)) return baseFetch;
	return (url, init) => {
		if (init?.redirect === "error" || init?.redirect === "manual") return baseFetch(url, init);
		return Promise.resolve(baseFetch(url, {
			...init,
			redirect: "manual"
		})).then((response) => followWithinOrigin(baseFetch, url, init, response) ?? response);
	};
}
/** @internal Describes a redirect that `fetchWithinOrigin` returned unfollowed, naming its target without userinfo, query or fragment. */
function unfollowedRedirect(response, requestUrl) {
	if (response.type === "opaqueredirect") return "Redirect not followed: this runtime does not expose the redirect target";
	const from = response.url || requestUrl;
	const target = redirectTarget(response, from);
	if (!target) return;
	target.username = target.password = target.search = target.hash = "";
	if (target.protocol === "http:" && new URL(from).protocol === "https:") {
		target.protocol = "https:";
		return `Redirect to plain http not followed; try ${target.href} instead`;
	}
	return `Redirect to ${target.href} not followed`;
}
// node_modules/.pnpm/@modelcontextprotocol+sdk@1.32.2_zod@4.6.7/node_modules/@modelcontextprotocol/sdk/dist/esm/client/auth.js
function isClientAuthMethod(method) {
	return [
		"client_secret_basic",
		"client_secret_post",
		"none"
	].includes(method);
}
const AUTHORIZATION_CODE_RESPONSE_TYPE = "code";
const AUTHORIZATION_CODE_CHALLENGE_METHOD = "S256";
/**
* Determines the best client authentication method to use based on server support and client configuration.
*
* Priority order (highest to lowest):
* 1. client_secret_basic (if client secret is available)
* 2. client_secret_post (if client secret is available)
* 3. none (for public clients)
*
* @param clientInformation - OAuth client information containing credentials
* @param supportedMethods - Authentication methods supported by the authorization server
* @returns The selected authentication method
*/
function selectClientAuthMethod(clientInformation, supportedMethods) {
	const hasClientSecret = clientInformation.client_secret !== void 0;
	if ("token_endpoint_auth_method" in clientInformation && clientInformation.token_endpoint_auth_method && isClientAuthMethod(clientInformation.token_endpoint_auth_method) && (supportedMethods.length === 0 || supportedMethods.includes(clientInformation.token_endpoint_auth_method))) return clientInformation.token_endpoint_auth_method;
	if (supportedMethods.length === 0) return hasClientSecret ? "client_secret_basic" : "none";
	if (hasClientSecret && supportedMethods.includes("client_secret_basic")) return "client_secret_basic";
	if (hasClientSecret && supportedMethods.includes("client_secret_post")) return "client_secret_post";
	if (supportedMethods.includes("none")) return "none";
	return hasClientSecret ? "client_secret_post" : "none";
}
/**
* Applies client authentication to the request based on the specified method.
*
* Implements OAuth 2.1 client authentication methods:
* - client_secret_basic: HTTP Basic authentication (RFC 6749 Section 2.3.1)
* - client_secret_post: Credentials in request body (RFC 6749 Section 2.3.1)
* - none: Public client authentication (RFC 6749 Section 2.1)
*
* @param method - The authentication method to use
* @param clientInformation - OAuth client information containing credentials
* @param headers - HTTP headers object to modify
* @param params - URL search parameters to modify
* @throws {Error} When required credentials are missing
*/
function applyClientAuthentication(method, clientInformation, headers, params) {
	const { client_id, client_secret } = clientInformation;
	switch (method) {
		case "client_secret_basic":
			applyBasicAuth(client_id, client_secret, headers);
			return;
		case "client_secret_post":
			applyPostAuth(client_id, client_secret, params);
			return;
		case "none":
			applyPublicAuth(client_id, params);
			return;
		default: throw new Error(`Unsupported client authentication method: ${method}`);
	}
}
/**
* Applies HTTP Basic authentication (RFC 6749 Section 2.3.1)
*/
function applyBasicAuth(clientId, clientSecret, headers) {
	if (!clientSecret) throw new Error("client_secret_basic authentication requires a client_secret");
	const credentials = btoa(`${clientId}:${clientSecret}`);
	headers.set("Authorization", `Basic ${credentials}`);
}
/**
* Applies POST body authentication (RFC 6749 Section 2.3.1)
*/
function applyPostAuth(clientId, clientSecret, params) {
	params.set("client_id", clientId);
	if (clientSecret) params.set("client_secret", clientSecret);
}
/**
* Applies public client authentication (RFC 6749 Section 2.1)
*/
function applyPublicAuth(clientId, params) {
	params.set("client_id", clientId);
}
/**
* Parses an OAuth error response from a string or Response object.
*
* If the input is a standard OAuth2.0 error response, it will be parsed according to the spec
* and an instance of the appropriate OAuthError subclass will be returned.
* If parsing fails, it falls back to a generic ServerError that includes
* the response status (if available) and original content.
*
* @param input - A Response object or string containing the error response
* @returns A Promise that resolves to an OAuthError instance
*/
async function parseErrorResponse(input) {
	const statusCode = input instanceof Response ? input.status : void 0;
	const body = input instanceof Response ? await input.text() : input;
	try {
		const { error, error_description, error_uri } = OAuthErrorResponseSchema.parse(JSON.parse(body));
		return new (OAUTH_ERRORS[error] || ServerError)(error_description || "", error_uri);
	} catch (error) {
		const redirect = input instanceof Response ? unfollowedRedirect(input, input.url) : void 0;
		return new ServerError(`${statusCode ? `HTTP ${statusCode}: ` : ""}${redirect ?? `Invalid OAuth error response: ${error}. Raw body: ${body}`}`);
	}
}
/**
* Compares two authorization server identifiers, tolerating a single trailing `/`
* difference (`String(new URL(...))` is slash-suffixed, advertised values often are not).
*/
function issuersMatch(a, b) {
	let [x, y] = [a, b];
	try {
		[x, y] = [new URL(a).href, new URL(b).href];
	} catch {}
	return x === y || x.endsWith("/") && x.slice(0, -1) === y || y.endsWith("/") && y.slice(0, -1) === x;
}
/**
* {@linkcode auth} stamps everything it passes to `saveClientInformation` / `saveTokens` with
* `issuer`, the authorization server URL used for discovery. This returns `stored` unless its
* stamp names a different authorization server, in which case the caller behaves as if nothing
* were stored. A value without a stamp (saved by an earlier version) is returned as-is, and
* is stamped once this authorization server has accepted it.
*/
function discardIfIssuerMismatch(stored, issuer) {
	if (!stored) return void 0;
	if (typeof stored.issuer !== "string") return stored.issuer == null ? stored : {
		...stored,
		issuer: void 0
	};
	return issuersMatch(stored.issuer, issuer) ? stored : void 0;
}
const TokenResponseSchema = OAuthTokensSchema.omit({ issuer: true });
const RegistrationResponseSchema = OAuthClientInformationFullSchema.omit({ issuer: true });
function boundElsewhereError(stored, issuer) {
	return /* @__PURE__ */ new Error(`OAuth client information is bound to authorization server ${stored.issuer} and is not presented to ${issuer}. Clear the stored client information, or correct \`expectedIssuer\`, if the authorization server has moved.`);
}
/**
* Orchestrates the full auth flow with a server.
*
* This can be used as a single entry point for all authorization functionality,
* instead of linking together the other lower-level functions in this module.
*/
async function auth(provider, options) {
	try {
		return await authInternal(provider, options);
	} catch (error) {
		if (error instanceof InvalidClientError || error instanceof UnauthorizedClientError) {
			await provider.invalidateCredentials?.("all");
			return await authInternal(provider, options);
		} else if (error instanceof InvalidGrantError) {
			await provider.invalidateCredentials?.("tokens");
			return await authInternal(provider, options);
		}
		throw error;
	}
}
async function authInternal(provider, { serverUrl, authorizationCode, scope, resourceMetadataUrl, fetchFn }) {
	const cachedState = await provider.discoveryState?.();
	let resourceMetadata;
	let authorizationServerUrl;
	let metadata;
	let effectiveResourceMetadataUrl = resourceMetadataUrl;
	if (!effectiveResourceMetadataUrl && cachedState?.resourceMetadataUrl) effectiveResourceMetadataUrl = new URL(cachedState.resourceMetadataUrl);
	if (cachedState?.authorizationServerUrl) {
		authorizationServerUrl = cachedState.authorizationServerUrl;
		resourceMetadata = cachedState.resourceMetadata;
		metadata = cachedState.authorizationServerMetadata ?? await discoverAuthorizationServerMetadata(authorizationServerUrl, { fetchFn });
		if (!resourceMetadata) try {
			resourceMetadata = await discoverOAuthProtectedResourceMetadata(serverUrl, { resourceMetadataUrl: effectiveResourceMetadataUrl }, fetchFn);
		} catch {}
		if (metadata !== cachedState.authorizationServerMetadata || resourceMetadata !== cachedState.resourceMetadata) await provider.saveDiscoveryState?.({
			authorizationServerUrl: String(authorizationServerUrl),
			resourceMetadataUrl: effectiveResourceMetadataUrl?.toString(),
			resourceMetadata,
			authorizationServerMetadata: metadata
		});
	} else {
		const serverInfo = await discoverOAuthServerInfo(serverUrl, {
			resourceMetadataUrl: effectiveResourceMetadataUrl,
			fetchFn
		});
		authorizationServerUrl = serverInfo.authorizationServerUrl;
		metadata = serverInfo.authorizationServerMetadata;
		resourceMetadata = serverInfo.resourceMetadata;
		await provider.saveDiscoveryState?.({
			authorizationServerUrl: String(authorizationServerUrl),
			resourceMetadataUrl: effectiveResourceMetadataUrl?.toString(),
			resourceMetadata,
			authorizationServerMetadata: metadata
		});
	}
	const issuer = String(authorizationServerUrl);
	const selectedResource = await selectResourceURL(serverUrl, provider, resourceMetadata);
	const resource = selectedResource && resourceMetadata && !provider.validateResourceURL ? resourceMetadata.resource : selectedResource;
	const resolvedScope = scope || resourceMetadata?.scopes_supported?.join(" ") || provider.clientMetadata.scope;
	const storedClientInformation = await Promise.resolve(provider.clientInformation());
	let clientInformation = discardIfIssuerMismatch(storedClientInformation, issuer);
	const canRegisterAgain = provider.saveClientInformation !== void 0 && !!provider.redirectUrl;
	if (storedClientInformation && !clientInformation && !canRegisterAgain) throw boundElsewhereError(storedClientInformation, issuer);
	const unstampedClientInformation = clientInformation?.issuer == null ? clientInformation : void 0;
	const bindClientInformation = async () => {
		try {
			if (unstampedClientInformation) await provider.saveClientInformation?.({
				...unstampedClientInformation,
				issuer
			});
		} catch {}
	};
	if (!clientInformation) {
		if (authorizationCode !== void 0) throw new Error("Existing OAuth client information is required when exchanging an authorization code");
		const supportsUrlBasedClientId = metadata?.client_id_metadata_document_supported === true;
		const clientMetadataUrl = provider.clientMetadataUrl;
		if (clientMetadataUrl && !isHttpsUrl(clientMetadataUrl)) throw new InvalidClientMetadataError(`clientMetadataUrl must be a valid HTTPS URL with a non-root pathname, got: ${clientMetadataUrl}`);
		if (supportsUrlBasedClientId && clientMetadataUrl) {
			clientInformation = {
				client_id: clientMetadataUrl,
				issuer
			};
			await provider.saveClientInformation?.(clientInformation);
		} else {
			if (!provider.saveClientInformation) throw new Error("OAuth client information must be saveable for dynamic registration");
			clientInformation = {
				...await registerClient(authorizationServerUrl, {
					metadata,
					clientMetadata: provider.clientMetadata,
					scope: resolvedScope,
					fetchFn
				}),
				issuer
			};
			await provider.saveClientInformation(clientInformation);
		}
	}
	const nonInteractiveFlow = !provider.redirectUrl;
	if (authorizationCode !== void 0 || nonInteractiveFlow) {
		const tokens = await fetchToken(provider, authorizationServerUrl, {
			metadata,
			resource,
			authorizationCode,
			fetchFn
		});
		await bindClientInformation();
		await provider.saveTokens({
			...tokens,
			issuer
		});
		return "AUTHORIZED";
	}
	const tokens = discardIfIssuerMismatch(await provider.tokens(), issuer);
	if (tokens?.refresh_token && tokens.issuer == null) console.warn("[mcp-sdk] stored OAuth tokens have no 'issuer' property (saved by an earlier version, or by a provider that does not keep it) and are used as-is; make sure your OAuthClientProvider stores what saveTokens() and saveClientInformation() receive unchanged.");
	if (tokens?.refresh_token) try {
		const newTokens = await refreshAuthorization(authorizationServerUrl, {
			metadata,
			clientInformation,
			refreshToken: tokens.refresh_token,
			resource,
			addClientAuthentication: provider.addClientAuthentication,
			fetchFn
		});
		await bindClientInformation();
		await provider.saveTokens({
			...newTokens,
			issuer
		});
		return "AUTHORIZED";
	} catch (error) {
		if (!(error instanceof OAuthError) || error instanceof ServerError) {} else throw error;
	}
	const state = provider.state ? await provider.state() : void 0;
	const { authorizationUrl, codeVerifier } = await startAuthorization(authorizationServerUrl, {
		metadata,
		clientInformation,
		state,
		redirectUrl: provider.redirectUrl,
		scope: resolvedScope,
		resource
	});
	await provider.saveCodeVerifier(codeVerifier);
	await provider.redirectToAuthorization(authorizationUrl);
	return "REDIRECT";
}
/**
* SEP-991: URL-based Client IDs
* Validate that the client_id is a valid URL with https scheme
*/
function isHttpsUrl(value) {
	if (!value) return false;
	try {
		const url = new URL(value);
		return url.protocol === "https:" && url.pathname !== "/";
	} catch {
		return false;
	}
}
/**
* Selects the RFC 8707 resource indicator for an MCP server: the provider's
* {@linkcode OAuthClientProvider.validateResourceURL | validateResourceURL} result when
* implemented, otherwise the protected resource metadata's `resource` (checked against the
* server URL with `checkResourceAllowed`), or `undefined` when there is no metadata.
*
* The result is a parsed `URL`, so a pathless indicator such as `https://example.com` has
* the `href` `https://example.com/`. {@linkcode auth} therefore sends the metadata string
* verbatim instead of this URL's `href` (#1968); callers that emit the `resource`
* parameter themselves should do the same.
*/
async function selectResourceURL(serverUrl, provider, resourceMetadata) {
	const defaultResource = resourceUrlFromServerUrl(serverUrl);
	if (provider.validateResourceURL) return await provider.validateResourceURL(defaultResource, resourceMetadata?.resource);
	if (!resourceMetadata) return;
	if (!checkResourceAllowed({
		requestedResource: defaultResource,
		configuredResource: resourceMetadata.resource
	})) throw new Error(`Protected resource ${resourceMetadata.resource} does not match expected ${defaultResource} (or origin)`);
	return new URL(resourceMetadata.resource);
}
/**
* Looks up RFC 9728 OAuth 2.0 Protected Resource Metadata.
*
* If the server returns a 404 for the well-known endpoint, this function will
* return `undefined`. Any other errors will be thrown as exceptions.
*/
async function discoverOAuthProtectedResourceMetadata(serverUrl, opts, fetchFn = fetch) {
	const response = await discoverMetadataWithFallback(serverUrl, "oauth-protected-resource", fetchFn, {
		protocolVersion: opts?.protocolVersion,
		metadataUrl: opts?.resourceMetadataUrl
	});
	if (!response || response.status === 404) {
		await response?.body?.cancel();
		throw new Error(`Resource server does not implement OAuth 2.0 Protected Resource Metadata.`);
	}
	if (!response.ok) {
		await response.body?.cancel();
		throw new Error(`HTTP ${response.status} trying to load well-known OAuth protected resource metadata.`);
	}
	return OAuthProtectedResourceMetadataSchema.parse(await response.json());
}
/**
* Helper function to handle fetch with CORS retry logic
*/
async function fetchWithCorsRetry(url, headers, fetchFn = fetch) {
	try {
		return await fetchWithinOrigin(fetchFn)(url, { headers });
	} catch (error) {
		if (error instanceof TypeError) {
			if (headers) return fetchWithCorsRetry(url, void 0, fetchFn);
			else return;
		}
		throw error;
	}
}
/**
* Constructs the well-known path for auth-related metadata discovery
*/
function buildWellKnownPath(wellKnownPrefix, pathname = "", options = {}) {
	if (pathname.endsWith("/")) pathname = pathname.slice(0, -1);
	return options.prependPathname ? `${pathname}/.well-known/${wellKnownPrefix}` : `/.well-known/${wellKnownPrefix}${pathname}`;
}
/**
* Tries to discover OAuth metadata at a specific URL
*/
async function tryMetadataDiscovery(url, protocolVersion, fetchFn = fetch) {
	return await fetchWithCorsRetry(url, { "MCP-Protocol-Version": protocolVersion }, fetchFn);
}
/**
* Determines if fallback to root discovery should be attempted
*/
function shouldAttemptFallback(response, pathname) {
	return !response || !response.ok && response.status < 500 && pathname !== "/";
}
/**
* Generic function for discovering OAuth metadata with fallback support
*/
async function discoverMetadataWithFallback(serverUrl, wellKnownType, fetchFn, opts) {
	const issuer = new URL(serverUrl);
	const protocolVersion = opts?.protocolVersion ?? "2025-11-25";
	let url;
	if (opts?.metadataUrl) url = new URL(opts.metadataUrl);
	else {
		const wellKnownPath = buildWellKnownPath(wellKnownType, issuer.pathname);
		url = new URL(wellKnownPath, opts?.metadataServerUrl ?? issuer);
		url.search = issuer.search;
	}
	let response = await tryMetadataDiscovery(url, protocolVersion, fetchFn);
	if (!opts?.metadataUrl && shouldAttemptFallback(response, issuer.pathname)) response = await tryMetadataDiscovery(new URL(`/.well-known/${wellKnownType}`, issuer), protocolVersion, fetchFn);
	return response;
}
/**
* Builds a list of discovery URLs to try for authorization server metadata.
* URLs are returned in priority order:
* 1. OAuth metadata at the given URL
* 2. OIDC metadata endpoints at the given URL
*/
function buildDiscoveryUrls(authorizationServerUrl) {
	const url = typeof authorizationServerUrl === "string" ? new URL(authorizationServerUrl) : authorizationServerUrl;
	const hasPath = url.pathname !== "/";
	const urlsToTry = [];
	if (!hasPath) {
		urlsToTry.push({
			url: new URL("/.well-known/oauth-authorization-server", url.origin),
			type: "oauth"
		});
		urlsToTry.push({
			url: new URL(`/.well-known/openid-configuration`, url.origin),
			type: "oidc"
		});
		return urlsToTry;
	}
	let pathname = url.pathname;
	if (pathname.endsWith("/")) pathname = pathname.slice(0, -1);
	urlsToTry.push({
		url: new URL(`/.well-known/oauth-authorization-server${pathname}`, url.origin),
		type: "oauth"
	});
	urlsToTry.push({
		url: new URL(`/.well-known/openid-configuration${pathname}`, url.origin),
		type: "oidc"
	});
	urlsToTry.push({
		url: new URL(`${pathname}/.well-known/openid-configuration`, url.origin),
		type: "oidc"
	});
	return urlsToTry;
}
/**
* Discovers authorization server metadata with support for RFC 8414 OAuth 2.0 Authorization Server Metadata
* and OpenID Connect Discovery 1.0 specifications.
*
* This function implements a fallback strategy for authorization server discovery:
* 1. Attempts RFC 8414 OAuth metadata discovery first
* 2. If OAuth discovery fails, falls back to OpenID Connect Discovery
*
* @param authorizationServerUrl - The authorization server URL obtained from the MCP Server's
*                                 protected resource metadata, or the MCP server's URL if the
*                                 metadata was not found.
* @param options - Configuration options
* @param options.fetchFn - Optional fetch function for making HTTP requests, defaults to global fetch
* @param options.protocolVersion - MCP protocol version to use, defaults to LATEST_PROTOCOL_VERSION
* @returns Promise resolving to authorization server metadata, or undefined if discovery fails
*/
async function discoverAuthorizationServerMetadata(authorizationServerUrl, { fetchFn = fetch, protocolVersion = LATEST_PROTOCOL_VERSION } = {}) {
	const headers = {
		"MCP-Protocol-Version": protocolVersion,
		Accept: "application/json"
	};
	const urlsToTry = buildDiscoveryUrls(authorizationServerUrl);
	for (const { url: endpointUrl, type } of urlsToTry) {
		const response = await fetchWithCorsRetry(endpointUrl, headers, fetchFn);
		if (!response)
 /**
		* CORS error occurred - don't throw as the endpoint may not allow CORS,
		* continue trying other possible endpoints
		*/
		continue;
		if (!response.ok) {
			await response.body?.cancel();
			if (response.status < 500) continue;
			throw new Error(`HTTP ${response.status} trying to load ${type === "oauth" ? "OAuth" : "OpenID provider"} metadata from ${endpointUrl}`);
		}
		if (type === "oauth") return OAuthMetadataSchema.parse(await response.json());
		else return OpenIdProviderDiscoveryMetadataSchema.parse(await response.json());
	}
}
/**
* Discovers the authorization server for an MCP server following
* {@link https://datatracker.ietf.org/doc/html/rfc9728 | RFC 9728} (OAuth 2.0 Protected
* Resource Metadata), with fallback to treating the server URL as the
* authorization server.
*
* This function combines two discovery steps into one call:
* 1. Probes `/.well-known/oauth-protected-resource` on the MCP server to find the
*    authorization server URL (RFC 9728).
* 2. Fetches authorization server metadata from that URL (RFC 8414 / OpenID Connect Discovery).
*
* Use this when you need the authorization server metadata for operations outside the
* {@linkcode auth} orchestrator, such as token refresh or token revocation.
*
* @param serverUrl - The MCP resource server URL
* @param opts - Optional configuration
* @param opts.resourceMetadataUrl - Override URL for the protected resource metadata endpoint
* @param opts.fetchFn - Custom fetch function for HTTP requests
* @returns Authorization server URL, metadata, and resource metadata (if available)
*/
async function discoverOAuthServerInfo(serverUrl, opts) {
	let resourceMetadata;
	let authorizationServerUrl;
	try {
		resourceMetadata = await discoverOAuthProtectedResourceMetadata(serverUrl, { resourceMetadataUrl: opts?.resourceMetadataUrl }, opts?.fetchFn);
		if (resourceMetadata.authorization_servers && resourceMetadata.authorization_servers.length > 0) authorizationServerUrl = resourceMetadata.authorization_servers[0];
	} catch {}
	if (!authorizationServerUrl) authorizationServerUrl = String(new URL("/", serverUrl));
	const authorizationServerMetadata = await discoverAuthorizationServerMetadata(authorizationServerUrl, { fetchFn: opts?.fetchFn });
	return {
		authorizationServerUrl,
		authorizationServerMetadata,
		resourceMetadata
	};
}
function resourceIndicatorToString(resource) {
	return typeof resource === "string" ? resource : resource.href;
}
/**
* Begins the authorization flow with the given server, by generating a PKCE challenge and constructing the authorization URL.
*/
async function startAuthorization(authorizationServerUrl, { metadata, clientInformation, redirectUrl, scope, state, resource }) {
	let authorizationUrl;
	if (metadata) {
		authorizationUrl = new URL(metadata.authorization_endpoint);
		if (!metadata.response_types_supported.includes(AUTHORIZATION_CODE_RESPONSE_TYPE)) throw new Error(`Incompatible auth server: does not support response type ${AUTHORIZATION_CODE_RESPONSE_TYPE}`);
		if (metadata.code_challenge_methods_supported && !metadata.code_challenge_methods_supported.includes(AUTHORIZATION_CODE_CHALLENGE_METHOD)) throw new Error(`Incompatible auth server: does not support code challenge method ${AUTHORIZATION_CODE_CHALLENGE_METHOD}`);
	} else authorizationUrl = new URL("/authorize", authorizationServerUrl);
	const challenge = await pkceChallenge();
	const codeVerifier = challenge.code_verifier;
	const codeChallenge = challenge.code_challenge;
	authorizationUrl.searchParams.set("response_type", AUTHORIZATION_CODE_RESPONSE_TYPE);
	authorizationUrl.searchParams.set("client_id", clientInformation.client_id);
	authorizationUrl.searchParams.set("code_challenge", codeChallenge);
	authorizationUrl.searchParams.set("code_challenge_method", AUTHORIZATION_CODE_CHALLENGE_METHOD);
	authorizationUrl.searchParams.set("redirect_uri", String(redirectUrl));
	if (state) authorizationUrl.searchParams.set("state", state);
	if (scope) authorizationUrl.searchParams.set("scope", scope);
	if (scope?.includes("offline_access")) authorizationUrl.searchParams.append("prompt", "consent");
	if (resource) authorizationUrl.searchParams.set("resource", resourceIndicatorToString(resource));
	return {
		authorizationUrl,
		codeVerifier
	};
}
/**
* Prepares token request parameters for an authorization code exchange.
*
* This is the default implementation used by fetchToken when the provider
* doesn't implement prepareTokenRequest.
*
* @param authorizationCode - The authorization code received from the authorization endpoint
* @param codeVerifier - The PKCE code verifier
* @param redirectUri - The redirect URI used in the authorization request
* @returns URLSearchParams for the authorization_code grant
*/
function prepareAuthorizationCodeRequest(authorizationCode, codeVerifier, redirectUri) {
	return new URLSearchParams({
		grant_type: "authorization_code",
		code: authorizationCode,
		code_verifier: codeVerifier,
		redirect_uri: String(redirectUri)
	});
}
/**
* Internal helper to execute a token request with the given parameters.
* Used by exchangeAuthorization, refreshAuthorization, and fetchToken.
*/
async function executeTokenRequest(authorizationServerUrl, { metadata, tokenRequestParams, clientInformation, addClientAuthentication, resource, fetchFn }) {
	const tokenUrl = metadata?.token_endpoint ? new URL(metadata.token_endpoint) : new URL("/token", authorizationServerUrl);
	const headers = new Headers({
		"Content-Type": "application/x-www-form-urlencoded",
		Accept: "application/json"
	});
	if (resource) tokenRequestParams.set("resource", resourceIndicatorToString(resource));
	if (addClientAuthentication) await addClientAuthentication(headers, tokenRequestParams, tokenUrl, metadata);
	else if (clientInformation) applyClientAuthentication(selectClientAuthMethod(clientInformation, metadata?.token_endpoint_auth_methods_supported ?? []), clientInformation, headers, tokenRequestParams);
	const response = await fetchWithinOrigin(fetchFn ?? fetch)(tokenUrl, {
		method: "POST",
		headers,
		body: tokenRequestParams
	});
	if (!response.ok) throw await parseErrorResponse(response);
	return TokenResponseSchema.parse(await response.json());
}
/**
* Exchange a refresh token for an updated access token.
*
* Supports multiple client authentication methods as specified in OAuth 2.1:
* - Automatically selects the best authentication method based on server support
* - Preserves the original refresh token if a new one is not returned
*
* @param authorizationServerUrl - The authorization server's base URL
* @param options - Configuration object containing client info, refresh token, etc.
* @returns Promise resolving to OAuth tokens (preserves original refresh_token if not replaced)
* @throws {Error} When token refresh fails or authentication is invalid
*/
async function refreshAuthorization(authorizationServerUrl, { metadata, clientInformation, refreshToken, resource, addClientAuthentication, fetchFn }) {
	return {
		refresh_token: refreshToken,
		...await executeTokenRequest(authorizationServerUrl, {
			metadata,
			tokenRequestParams: new URLSearchParams({
				grant_type: "refresh_token",
				refresh_token: refreshToken
			}),
			clientInformation,
			addClientAuthentication,
			resource,
			fetchFn
		})
	};
}
/**
* Unified token fetching that works with any grant type via provider.prepareTokenRequest().
*
* This function provides a single entry point for obtaining tokens regardless of the
* OAuth grant type. The provider's prepareTokenRequest() method determines which grant
* to use and supplies the grant-specific parameters.
*
* @param provider - OAuth client provider that implements prepareTokenRequest()
* @param authorizationServerUrl - The authorization server's base URL
* @param options - Configuration for the token request
* @returns Promise resolving to OAuth tokens
* @throws {Error} When provider doesn't implement prepareTokenRequest or token fetch fails
*
* @example
* // Provider for client_credentials:
* class MyProvider implements OAuthClientProvider {
*   prepareTokenRequest(scope) {
*     const params = new URLSearchParams({ grant_type: 'client_credentials' });
*     if (scope) params.set('scope', scope);
*     return params;
*   }
*   // ... other methods
* }
*
* const tokens = await fetchToken(provider, authServerUrl, { metadata });
*/
async function fetchToken(provider, authorizationServerUrl, { metadata, resource, authorizationCode, fetchFn } = {}) {
	const readClientInformation = async () => {
		const storedClientInformation = await provider.clientInformation();
		const checked = discardIfIssuerMismatch(storedClientInformation, String(authorizationServerUrl));
		if (storedClientInformation && !checked) throw boundElsewhereError(storedClientInformation, String(authorizationServerUrl));
		return checked;
	};
	let clientInformation = await readClientInformation();
	const scope = provider.clientMetadata.scope;
	let tokenRequestParams;
	if (provider.prepareTokenRequest) tokenRequestParams = await provider.prepareTokenRequest(scope);
	if (!tokenRequestParams) {
		if (!authorizationCode) throw new Error("Either provider.prepareTokenRequest() or authorizationCode is required");
		if (!provider.redirectUrl) throw new Error("redirectUrl is required for authorization_code flow");
		tokenRequestParams = prepareAuthorizationCodeRequest(authorizationCode, await provider.codeVerifier(), provider.redirectUrl);
	}
	clientInformation ?? (clientInformation = await readClientInformation());
	return executeTokenRequest(authorizationServerUrl, {
		metadata,
		tokenRequestParams,
		clientInformation,
		addClientAuthentication: provider.addClientAuthentication,
		resource,
		fetchFn
	});
}
/**
* Performs OAuth 2.0 Dynamic Client Registration according to RFC 7591.
*
* If `scope` is provided, it overrides `clientMetadata.scope` in the registration
* request body. This allows callers to apply the Scope Selection Strategy (SEP-835)
* consistently across both DCR and the subsequent authorization request.
*/
async function registerClient(authorizationServerUrl, { metadata, clientMetadata, scope, fetchFn }) {
	let registrationUrl;
	if (metadata) {
		if (!metadata.registration_endpoint) throw new Error("Incompatible auth server: does not support dynamic client registration");
		registrationUrl = new URL(metadata.registration_endpoint);
	} else registrationUrl = new URL("/register", authorizationServerUrl);
	const response = await fetchWithinOrigin(fetchFn ?? fetch)(registrationUrl, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({
			...clientMetadata,
			...scope !== void 0 ? { scope } : {}
		})
	});
	if (!response.ok) throw await parseErrorResponse(response);
	return RegistrationResponseSchema.parse(await response.json());
}
// src/config.ts
const PROVIDERS = [
	["claudeAgent", "Claude"],
	["codex", "Codex"],
	["opencode", "OpenCode"],
	["cursor", "Cursor"],
	["grok", "Grok"],
	["antigravity", "Antigravity"],
	["kiro", "Kiro"],
	["muse", "Muse"],
	["fx", "fx"]
];
const initialState = () => ({
	version: 2,
	servers: []
});
function text(value, name, max = 16384) {
	if (typeof value !== "string" || value.length > max || value.includes("\0")) throw Error(`Invalid ${name}`);
	return value.trim();
}
function record(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw Error("Expected an object");
	return value;
}
function stringMap(value) {
	const result = {};
	for (const [key, item] of Object.entries(record(value))) {
		if (!key || key.length > 200 || /[\0\r\n]/.test(key) || typeof item !== "string" || item.length > 16384 || item.includes("\0")) throw Error("Invalid environment or header value");
		Object.defineProperty(result, key, {
			value: item,
			enumerable: true,
			writable: true,
			configurable: true
		});
	}
	if (Object.keys(result).length > 100) throw Error("Too many environment or header values");
	return result;
}
function directory(value) {
	const path = text(value, "project directory");
	if (!Path.isAbsolute(path)) throw Error("Project directory must be absolute");
	return Path.normalize(path);
}
function parseServer(input, id, previous) {
	const name = text(input.name, "server name", 48);
	if (!/^[a-z][a-z0-9-]*$/.test(name)) throw Error("Use a lowercase server name with letters, numbers and hyphens");
	const label = text(input.label ?? name, "label", 200) || name;
	const project = input.scope === "project" ? directory(input.project) : input.scope === "global" ? null : (() => {
		throw Error("Invalid scope");
	})();
	const transport = input.transport;
	if (transport !== "stdio" && transport !== "http" && transport !== "sse") throw Error("Invalid transport");
	const auth = input.auth ?? previous?.auth ?? "static";
	if (auth !== "static" && auth !== "oauth") throw Error("Invalid authentication mode");
	if (auth === "oauth" && transport !== "http") throw Error("OAuth requires Streamable HTTP");
	const command = text(input.command ?? "", "command");
	let args;
	try {
		args = typeof input.args === "string" ? JSON.parse(input.args || "[]") : input.args ?? [];
	} catch {
		throw Error("Arguments must be a JSON array");
	}
	if (!Array.isArray(args) || args.length > 100 || args.some((item) => typeof item !== "string" || item.length > 16384 || item.includes("\0"))) throw Error("Invalid command arguments");
	if (transport === "stdio" && !command) throw Error("A command is required for stdio servers");
	let url = text(input.url ?? "", "URL");
	if (transport !== "stdio") {
		const parsed = new URL(url);
		if (!["http:", "https:"].includes(parsed.protocol) || parsed.username || parsed.password || parsed.hash) throw Error("Use an HTTP(S) URL without embedded credentials or fragments");
		url = parsed.href;
	}
	const providers = { ...previous?.providers ?? {} };
	for (const [provider] of PROVIDERS) {
		const value = input[`provider-${provider.toLowerCase()}`];
		if (value !== void 0 && typeof value !== "boolean") throw Error("Invalid provider selection");
		providers[provider] = value ?? providers[provider] ?? true;
	}
	return {
		id,
		name,
		label,
		project,
		enabled: previous?.enabled ?? true,
		providers,
		transport,
		command,
		args,
		url,
		secretKey: previous?.secretKey ?? `server-${id}`,
		auth
	};
}
function decodeState(value) {
	const input = record(value);
	if (input.version !== 1 && input.version !== 2 || !Array.isArray(input.servers) || input.servers.length > 40) throw Error("Invalid MCP manager state");
	const servers = input.servers.map((value) => {
		const row = record(value);
		const id = text(row.id, "id", 80);
		if (!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(id)) throw Error("Invalid server id");
		const server = parseServer({
			...row,
			scope: row.project === null ? "global" : "project"
		}, id);
		if (typeof row.enabled !== "boolean" || typeof row.secretKey !== "string" || !/^server-[a-f0-9]{32}$/.test(row.secretKey)) throw Error("Invalid server metadata");
		const providers = Object.fromEntries(Object.entries(record(row.providers)).map(([key, enabled]) => {
			if (!/^[a-zA-Z][a-zA-Z0-9_.:-]{0,127}$/.test(key) || typeof enabled !== "boolean") throw Error("Invalid provider rule");
			return [key, enabled];
		}));
		return {
			...server,
			secretKey: row.secretKey,
			enabled: row.enabled,
			providers
		};
	});
	if (new Set(servers.map((s) => s.id)).size !== servers.length || new Set(servers.map((s) => JSON.stringify([s.project, s.name]))).size !== servers.length) throw Error("Duplicate MCP server");
	return {
		version: 2,
		servers
	};
}
function selectedServers(state, launch, project) {
	const applicable = new Map(state.servers.filter((s) => s.project === null).map((s) => [s.name, s]));
	if (project) {
		for (const server of state.servers) if (server.project === project) applicable.set(server.name, server);
	}
	return [...applicable.values()].filter((s) => s.enabled && (s.providers[launch.instanceId ?? ""] ?? s.providers[launch.provider] ?? PROVIDERS.some(([id]) => id === launch.provider)));
}
function serverConfig(server, credentials) {
	return server.transport === "stdio" ? {
		transport: "stdio",
		command: server.command,
		args: server.args,
		environment: credentials.environment
	} : {
		transport: server.transport,
		url: server.url,
		headers: credentials.headers
	};
}
// src/oauth.ts
const opaque = (value, name, max = 16384) => {
	if (typeof value !== "string" || value.length > max || value.includes("\0")) throw Error(`Invalid ${name}`);
	return value;
};
const random = () => randomBytes(32).toString("base64url");
const key = (id) => `oauth-${id.replaceAll("-", "")}`;
const equal = (a, b) => {
	const first = Buffer.from(a), second = Buffer.from(b);
	return first.length === second.length && timingSafeEqual(first, second);
};
const identity = (server) => JSON.stringify([
	server.id,
	server.project,
	server.url,
	server.auth
]);
const expiry = (tokens, now) => {
	if (tokens.token_type.toLowerCase() !== "bearer") throw Error("Only OAuth bearer tokens are supported");
	if (tokens.expires_in === void 0) return null;
	const result = now + tokens.expires_in * 1e3;
	if (tokens.expires_in < 0 || !Number.isFinite(result)) throw Error("Invalid OAuth token expiry");
	return result;
};
const FLOW_LIFETIME = 6e5;
const MAX_BODY = 8388608;
const headers = {
	"Cache-Control": "no-store",
	"Content-Type": "text/plain; charset=utf-8",
	"X-Content-Type-Options": "nosniff"
};
const reply = (message, status = 200) => new Response(message, {
	status,
	headers
});
function secureUrl(value) {
	const url = new URL(value);
	if (url.username || url.password || url.hash || url.protocol !== "https:" && !(url.protocol === "http:" && [
		"localhost",
		"127.0.0.1",
		"[::1]"
	].includes(url.hostname))) throw Error("OAuth requires HTTPS or a loopback HTTP URL without credentials");
	return url;
}
function discovery(value) {
	const issuer = secureUrl(value.authorizationServerUrl);
	const metadata = OAuthMetadataSchema.parse(value.authorizationServerMetadata);
	const resource = OAuthProtectedResourceMetadataSchema.parse(value.resourceMetadata);
	if (secureUrl(metadata.issuer).href !== issuer.href || !resource.authorization_servers?.some((url) => secureUrl(url).href === issuer.href)) throw Error("OAuth issuer does not match the selected authorization server");
	if (!metadata.code_challenge_methods_supported?.includes("S256")) throw Error("OAuth requires S256 PKCE");
	secureUrl(metadata.authorization_endpoint);
	secureUrl(metadata.token_endpoint);
	return {
		...value,
		authorizationServerMetadata: metadata,
		resourceMetadata: resource
	};
}
function decodeGrant(value) {
	const data = record(JSON.parse(value));
	if (data.version !== 1 || typeof data.identity !== "string" || typeof data.label !== "string" || data.label.length > 120 || typeof data.redirectUrl !== "string" || typeof data.routeToken !== "string" || !/^[A-Za-z0-9_-]{43}$/.test(data.routeToken) || !(data.expiresAt === null || typeof data.expiresAt === "number" && Number.isFinite(data.expiresAt))) throw Error("Invalid OAuth connection state");
	return {
		version: 1,
		identity: data.identity,
		label: data.label,
		redirectUrl: secureUrl(data.redirectUrl).href,
		routeToken: data.routeToken,
		...data.scope === void 0 ? {} : { scope: text(data.scope, "OAuth scopes", 2e3) },
		expiresAt: data.expiresAt,
		client: {
			...record(data.client),
			...OAuthClientInformationSchema.parse(data.client)
		},
		tokens: OAuthTokensSchema.parse(data.tokens),
		discovery: discovery(data.discovery)
	};
}
async function createOAuth(context, store, options = {}) {
	const fetcher = options.fetch ?? fetch;
	const now = options.now ?? Date.now;
	const flows = /* @__PURE__ */ new Map();
	const sessions = /* @__PURE__ */ new Map();
	const controllers = /* @__PURE__ */ new Map();
	const queues = /* @__PURE__ */ new Map();
	const changed = options.changed ?? (async () => {});
	const shutdown = new AbortController();
	let closed = false;
	const serial = (id, work) => {
		const next = (queues.get(id) ?? Promise.resolve()).then(work);
		const settled = next.catch(() => void 0).finally(() => {
			if (queues.get(id) === settled) queues.delete(id);
		});
		queues.set(id, settled);
		return next;
	};
	const get = async (id) => {
		if (closed) throw Error("OAuth manager is closed");
		const server = await store.get(id);
		if (!server || server.auth !== "oauth" || server.transport !== "http") throw Error("OAuth server is unavailable");
		secureUrl(server.url);
		return server;
	};
	const load = async (server) => {
		const value = await runPromise(context.secrets.get(key(server.id)));
		if (value === null) return;
		const grant = decodeGrant(value);
		if (grant.identity !== identity(server)) throw Error("OAuth connection belongs to different server settings");
		const issuer = secureUrl(grant.discovery.authorizationServerUrl).href;
		if (!grant.tokens.issuer || !grant.client.issuer || secureUrl(grant.tokens.issuer).href !== issuer || secureUrl(grant.client.issuer).href !== issuer) throw Error("OAuth credentials have a different issuer");
		return grant;
	};
	const abort = (id) => {
		controllers.get(id)?.abort();
		controllers.delete(id);
		for (const [session, value] of sessions) if (value.owner === id) sessions.delete(session);
	};
	const control = (id) => {
		let controller = controllers.get(id);
		if (!controller) {
			controller = new AbortController();
			controllers.set(id, controller);
		}
		return controller;
	};
	const save = async (server, grant, revision) => {
		const current = await get(server.id);
		if (!current.enabled || identity(current) !== grant.identity || revision && current.secretKey !== revision) throw Error("Server changed during OAuth sign-in");
		await runPromise(context.secrets.set(key(server.id), JSON.stringify(grant)));
	};
	const provider = (flow, tokens) => ({
		redirectUrl: flow.redirectUrl,
		...flow.clientMetadataUrl ? { clientMetadataUrl: flow.clientMetadataUrl } : {},
		clientMetadata: {
			client_name: "T3 MCP Manager",
			redirect_uris: [flow.redirectUrl],
			grant_types: ["authorization_code", "refresh_token"],
			response_types: ["code"],
			token_endpoint_auth_method: flow.client?.client_secret ? "client_secret_post" : "none",
			...flow.scope ? { scope: flow.scope } : {}
		},
		state: () => flow.state,
		clientInformation: () => flow.client,
		saveClientInformation: (value) => {
			flow.client = value;
		},
		tokens: () => tokens,
		saveTokens: (value) => {
			flow.tokens = value;
		},
		saveCodeVerifier: (value) => {
			flow.verifier = value;
		},
		codeVerifier: () => {
			if (!flow.verifier) throw Error("OAuth verifier is unavailable");
			return flow.verifier;
		},
		redirectToAuthorization: (url) => {
			if (tokens) throw Error("OAuth connection needs to be reconnected");
			if (flow.chooseAccount) url.searchParams.set("prompt", "select_account");
			const authorizationUrl = secureUrl(url.href).href;
			if (authorizationUrl.length > 16384) throw Error("OAuth authorization URL is too long");
			flow.authorizationUrl = authorizationUrl;
		},
		discoveryState: () => flow.discovery,
		saveDiscoveryState: (value) => {
			const state = discovery(value);
			if (flow.client?.issuer && secureUrl(flow.client.issuer).href !== secureUrl(state.authorizationServerUrl).href) throw Error("Registered client issuer does not match this server");
			flow.discovery = state;
		},
		invalidateCredentials: (scope) => {
			if (scope === "all" || scope === "client") delete flow.client;
			if (scope === "all" || scope === "tokens") {
				delete flow.tokens;
				tokens = void 0;
			}
			if (scope === "all" || scope === "verifier") delete flow.verifier;
			if (scope === "all" || scope === "discovery") delete flow.discovery;
		}
	});
	const authorize = (flow, serverUrl, tokens, code) => {
		const deadline = AbortSignal.timeout(2e4);
		const fetchFn = async (input, init) => {
			secureUrl(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
			const response = await fetcher(input, {
				...init,
				redirect: "error",
				signal: AbortSignal.any([
					shutdown.signal,
					deadline,
					...init?.signal ? [init.signal] : []
				])
			});
			if (!response.body) return response;
			const reader = response.body.getReader(), chunks = [];
			let size = 0;
			try {
				for (;;) {
					const part = await reader.read();
					if (part.done) break;
					size += part.value.byteLength;
					if (size > 262144) {
						await reader.cancel();
						throw Error("OAuth response is too large");
					}
					chunks.push(part.value);
				}
			} finally {
				reader.releaseLock();
			}
			const buffer = new Uint8Array(size);
			let offset = 0;
			for (const chunk of chunks) {
				buffer.set(chunk, offset);
				offset += chunk.byteLength;
			}
			return new Response(buffer, {
				status: response.status,
				headers: response.headers
			});
		};
		return auth(provider(flow, tokens), {
			serverUrl,
			...code === void 0 ? {} : { authorizationCode: code },
			...flow.scope ? { scope: flow.scope } : {},
			fetchFn
		});
	};
	const prune = () => {
		for (const [id, flow] of flows) if (now() - flow.createdAt >= FLOW_LIFETIME) flows.delete(id);
	};
	const finish = async (id, callback, automatic = false) => serial(id, async () => {
		prune();
		const flow = flows.get(id);
		if (!flow) throw Error("OAuth sign-in expired or was cancelled");
		const url = new URL(callback), expected = new URL(flow.redirectUrl);
		if (!automatic && url.origin !== expected.origin || url.pathname !== expected.pathname || url.hash !== "" || url.searchParams.getAll("state").length !== 1 || !equal(url.searchParams.get("state") ?? "", flow.state)) throw Error("OAuth callback does not match this project");
		const server = await get(id);
		if (server.secretKey !== flow.revision || identity(server) !== flow.identity) throw Error("Server changed during OAuth sign-in");
		if (url.searchParams.has("error")) {
			flows.delete(id);
			await changed();
			throw Error("OAuth authorization was declined");
		}
		const code = url.searchParams.get("code");
		if (!code || url.searchParams.getAll("code").length !== 1) throw Error("OAuth authorization code is missing");
		const expectedIssuer = flow.discovery?.authorizationServerMetadata?.issuer;
		const suppliedIssuer = url.searchParams.get("iss");
		if (url.searchParams.getAll("iss").length > 1) throw Error("OAuth response issuer does not match");
		if (flow.discovery?.authorizationServerMetadata && "authorization_response_iss_parameter_supported" in flow.discovery.authorizationServerMetadata && flow.discovery.authorizationServerMetadata.authorization_response_iss_parameter_supported === true && !suppliedIssuer || suppliedIssuer && (!expectedIssuer || secureUrl(suppliedIssuer).href !== secureUrl(expectedIssuer).href)) throw Error("OAuth response issuer does not match");
		flows.delete(id);
		if (await authorize(flow, server.url, void 0, code) !== "AUTHORIZED" || !flow.tokens || !flow.client || !flow.discovery) throw Error("OAuth sign-in did not return credentials");
		const grant = {
			version: 1,
			identity: flow.identity,
			label: flow.label,
			redirectUrl: flow.redirectUrl,
			client: flow.client,
			tokens: flow.tokens,
			discovery: flow.discovery,
			expiresAt: expiry(flow.tokens, now()),
			routeToken: random(),
			...flow.scope ?? flow.tokens.scope ? { scope: flow.scope ?? flow.tokens.scope } : {}
		};
		await save(server, grant, flow.revision);
		abort(id);
		await changed();
		return {
			id,
			connected: true
		};
	});
	const refresh = (server, force = false, rejectedToken) => serial(server.id, async () => {
		const grant = await load(server);
		if (!grant) throw Error("Connect an OAuth account first");
		if (force && rejectedToken !== void 0 && grant.tokens.access_token !== rejectedToken || !force && (grant.expiresAt === null || grant.expiresAt > now() + 6e4)) return grant;
		const flow = {
			id: server.id,
			revision: server.secretKey,
			identity: identity(server),
			state: random(),
			redirectUrl: grant.redirectUrl,
			label: grant.label,
			createdAt: now(),
			client: grant.client,
			discovery: grant.discovery,
			chooseAccount: false,
			...grant.scope ? { scope: grant.scope } : {}
		};
		if (await authorize(flow, server.url, grant.tokens) !== "AUTHORIZED" || !flow.tokens || !flow.client || !flow.discovery) throw Error("Reconnect this OAuth account");
		const tokens = {
			...flow.tokens,
			...!flow.tokens.refresh_token && grant.tokens.refresh_token ? { refresh_token: grant.tokens.refresh_token } : {}
		};
		const next = {
			...grant,
			client: flow.client,
			tokens,
			discovery: flow.discovery,
			expiresAt: expiry(tokens, now())
		};
		await save(server, next);
		await changed();
		return next;
	});
	return {
		start: (input) => serial(text(input.id, "server id", 80), async () => {
			prune();
			const id = text(input.id, "server id", 80), server = await get(id);
			if (!server.enabled) throw Error("Enable the server before connecting OAuth");
			if (!flows.has(id) && flows.size >= 10) throw Error("At most ten sign-ins can be pending");
			const base = await runPromise(context.httpBaseUrl);
			const redirect = input["redirect-url"] ? secureUrl(text(input["redirect-url"], "redirect URL")) : new URL(base.replace(/\/$/, "") + "/oauth/callback");
			if (redirect.search || redirect.hash) throw Error("Callback URL cannot contain query parameters or fragments");
			const existing = await load(server).catch(() => void 0);
			const clientId = opaque(input["client-id"] ?? "", "client ID", 2e3);
			const clientSecret = opaque(input["client-secret"] ?? "", "client secret");
			const chooseAccount = input["choose-account"];
			if (chooseAccount !== void 0 && typeof chooseAccount !== "boolean") throw Error("Invalid account-selection preference");
			if (clientSecret && !clientId) throw Error("A client ID is required with a client secret");
			const clientIssuer = input.issuer ? secureUrl(text(input.issuer, "registered client issuer", 2e3)).href : void 0;
			if (clientSecret && !clientIssuer) throw Error("A registered client issuer is required with a client secret");
			const flow = {
				id,
				revision: server.secretKey,
				identity: identity(server),
				state: random(),
				redirectUrl: redirect.href,
				label: text(input.label ?? "Connected account", "account label", 120) || "Connected account",
				createdAt: now(),
				chooseAccount: chooseAccount ?? true,
				...clientId ? { client: {
					client_id: clientId,
					...clientIssuer ? { issuer: clientIssuer } : {},
					...clientSecret ? { client_secret: clientSecret } : {}
				} } : existing?.redirectUrl === redirect.href ? { client: existing.client } : {},
				...input.scopes ? { scope: text(input.scopes, "scopes", 2e3) } : existing?.scope ? { scope: existing.scope } : {},
				...input["client-metadata-url"] ? { clientMetadataUrl: secureUrl(text(input["client-metadata-url"], "client metadata URL")).href } : {}
			};
			if (await authorize(flow, server.url) !== "REDIRECT" || !flow.authorizationUrl || !flow.verifier) throw Error("OAuth sign-in did not produce an authorization URL");
			if (closed) throw Error("OAuth manager is closed");
			flows.set(id, flow);
			await changed();
			return {
				id,
				started: true
			};
		}),
		finish: (input) => finish(text(input.id, "server id", 80), text(input.callback, "callback URL")),
		cancel: (id) => serial(id, async () => {
			flows.delete(id);
			await changed();
			return { cancelled: true };
		}),
		disconnect: (id) => serial(id, async () => {
			flows.delete(id);
			abort(id);
			await runPromise(context.secrets.remove(key(id)));
			await changed();
			return { disconnected: true };
		}),
		policyChanged: (id) => serial(id, async () => {
			flows.delete(id);
			const server = await store.get(id);
			const grant = server?.auth === "oauth" ? await load(server).catch(() => void 0) : void 0;
			if (!server || !server.enabled || server.auth !== "oauth" || !grant) abort(id);
			if (!server || server.auth !== "oauth") await runPromise(context.secrets.remove(key(id)));
			await changed();
		}),
		status: async () => {
			prune();
			const status = [];
			for (const server of (await store.list()).servers.filter((server) => server.auth === "oauth")) {
				let problem = false;
				const grant = await load(server).catch(() => {
					problem = true;
				});
				const flow = flows.get(server.id);
				status.push({
					id: server.id,
					label: server.label,
					project: server.project,
					connected: !!grant,
					problem,
					account: grant?.label ?? "Not connected",
					pending: !!flow,
					...flow?.authorizationUrl ? {
						authorizationUrl: flow.authorizationUrl,
						redirectUrl: flow.redirectUrl
					} : {}
				});
			}
			return status;
		},
		resolve: async (server) => {
			const grant = await load(server);
			if (!grant) throw Error("Connect an OAuth account for this project first");
			return {
				transport: "http",
				url: `${(await runPromise(context.httpBaseUrl)).replace(/\/$/, "")}/mcp/${server.id}`,
				headers: { Authorization: `Bearer ${grant.routeToken}` }
			};
		},
		handleHttp: async (request) => {
			const url = new URL(request.url), base = context.httpBasePath.replace(/\/$/, "");
			if (url.pathname === `${base}/oauth/callback` && request.method === "GET") {
				prune();
				const state = url.searchParams.get("state"), flow = state ? [...flows.values()].find((flow) => equal(flow.state, state)) : void 0;
				if (!flow) return reply("This sign-in is invalid, expired or already completed.", 400);
				try {
					await finish(flow.id, url.href, true);
					return reply("Connected. Return to MCP Manager in T3 Code.");
				} catch {
					return reply("Sign-in could not be completed. Return to MCP Manager and try again.", 400);
				}
			}
			const match = new RegExp(`^${base}/mcp/([a-f0-9-]{36})$`).exec(url.pathname);
			if (!match || url.search) return reply("Not found", 404);
			const id = match[1];
			let server, grant;
			try {
				server = await get(id);
				const current = await load(server);
				if (!current || !server.enabled || !equal(request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") ?? "", current.routeToken)) return reply("Forbidden", 403);
				grant = await refresh(server);
			} catch {
				return reply("Reconnect this project's account in MCP Manager.", 403);
			}
			if (![
				"GET",
				"POST",
				"DELETE"
			].includes(request.method)) return reply("Method not allowed", 405);
			const frontSession = request.headers.get("mcp-session-id"), session = frontSession ? sessions.get(frontSession) : void 0;
			if (frontSession && (!session || session.owner !== id)) return reply("MCP session not found", 404);
			let body;
			if (request.body) {
				const reader = request.body.getReader(), chunks = [];
				let size = 0;
				try {
					for (;;) {
						const part = await reader.read();
						if (part.done) break;
						size += part.value.byteLength;
						if (size > MAX_BODY) {
							await reader.cancel();
							return reply("MCP request is too large", 413);
						}
						chunks.push(part.value);
					}
				} finally {
					reader.releaseLock();
				}
				const buffer = new Uint8Array(size);
				let offset = 0;
				for (const part of chunks) {
					buffer.set(part, offset);
					offset += part.byteLength;
				}
				body = buffer.buffer;
			}
			const additional = await store.credentials(server);
			const upstreamHeaders = new Headers(additional.headers);
			for (const name of [
				"host",
				"connection",
				"content-length",
				"cookie",
				"proxy-authorization",
				"authorization",
				"mcp-session-id"
			]) upstreamHeaders.delete(name);
			for (const name of [
				"content-type",
				"accept",
				"mcp-protocol-version",
				"last-event-id"
			]) {
				const value = request.headers.get(name);
				if (value) upstreamHeaders.set(name, value);
			}
			if (session) upstreamHeaders.set("mcp-session-id", session.upstream);
			const signal = AbortSignal.any([request.signal, control(id).signal]);
			const send = (token) => {
				upstreamHeaders.set("authorization", `Bearer ${token}`);
				return fetcher(server.url, {
					method: request.method,
					headers: upstreamHeaders,
					redirect: "manual",
					signal,
					...body === void 0 ? {} : { body }
				});
			};
			let response;
			try {
				response = await send(grant.tokens.access_token);
			} catch {
				return reply("MCP server is unavailable.", 502);
			}
			if (response.status === 401) {
				await response.body?.cancel();
				try {
					grant = await refresh(server, true, grant.tokens.access_token);
					response = await send(grant.tokens.access_token);
				} catch {
					return reply("Reconnect this project's account in MCP Manager.", 403);
				}
			}
			if (response.status === 401 || response.status === 403) {
				await response.body?.cancel();
				return reply("Reconnect this project's account in MCP Manager.", 403);
			}
			if (response.status >= 300 && response.status < 400) {
				await response.body?.cancel();
				return reply("MCP endpoint redirected; update its URL in MCP Manager.", 502);
			}
			const output = new Headers({ "Cache-Control": "no-store" });
			for (const name of ["content-type", "mcp-protocol-version"]) {
				const value = response.headers.get(name);
				if (value) output.set(name, value);
			}
			const upstreamSession = response.headers.get("mcp-session-id");
			if (upstreamSession) {
				const front = session && frontSession ? frontSession : random();
				if (!sessions.has(front) && sessions.size >= 1024) sessions.delete(sessions.keys().next().value);
				sessions.set(front, {
					owner: id,
					upstream: upstreamSession
				});
				output.set("mcp-session-id", front);
			}
			if (request.method === "DELETE" && frontSession) sessions.delete(frontSession);
			return new Response(response.body, {
				status: response.status,
				headers: output
			});
		},
		close: async () => {
			closed = true;
			shutdown.abort();
			flows.clear();
			sessions.clear();
			for (const controller of controllers.values()) controller.abort();
			controllers.clear();
		}
	};
}
// src/oauthView.ts
function oauthBlocks(accounts, callback) {
	const blocks = [{
		id: "oauth-help",
		kind: "text",
		text: `OAuth accounts belong to a server definition and its project. To use different accounts at the same MCP URL, add one project-scoped definition per project with Authentication set to OAuth. A global connection is shared only by projects that inherit that global definition. Tokens stay on this environment. Default callback URL: ${callback}`
	}];
	if (accounts.length === 0) return blocks;
	blocks.push({
		id: "oauth-accounts",
		kind: "table",
		columns: [
			"Server",
			"Project",
			"Account label",
			"Connection"
		],
		rows: accounts.map((account) => ({
			id: account.id,
			cells: [
				account.label,
				account.project ?? "All projects (default)",
				account.account,
				account.pending ? "Waiting for sign-in" : account.connected ? "Connected" : account.problem ? "Reconnect required" : "Not connected"
			],
			actions: [
				{
					id: "connect",
					label: account.connected ? "Use another account" : "Sign in",
					command: "oauth-start",
					input: { id: account.id }
				},
				...account.pending ? [{
					id: "cancel",
					label: "Cancel sign-in",
					command: "oauth-cancel",
					input: { id: account.id }
				}] : [],
				...account.connected || account.problem ? [{
					id: "disconnect",
					label: "Disconnect this account",
					command: "oauth-disconnect",
					input: { id: account.id }
				}] : []
			]
		}))
	});
	blocks.push({
		id: "oauth-connect",
		kind: "form",
		label: "Connect an OAuth account",
		command: "oauth-start",
		input: {},
		fields: [
			{
				name: "id",
				label: "Server and project",
				kind: "select",
				required: true,
				options: accounts.map((account) => ({
					value: account.id,
					label: `${account.label} (${account.project ?? "all projects"})`.slice(0, 200)
				}))
			},
			{
				name: "label",
				label: "Account label (for example, Personal or Work)",
				kind: "text"
			},
			{
				name: "choose-account",
				label: "Ask the provider to choose an account",
				kind: "boolean",
				value: true
			},
			{
				name: "scopes",
				label: "OAuth scopes (optional, space-separated)",
				kind: "text"
			},
			{
				name: "client-id",
				label: "Registered client ID (optional; otherwise register automatically)",
				kind: "text"
			},
			{
				name: "issuer",
				label: "Registered client's authorization server issuer (required with a secret)",
				kind: "text"
			},
			{
				name: "client-secret",
				label: "Registered client secret (optional, write-only)",
				kind: "password"
			},
			{
				name: "client-metadata-url",
				label: "Public client metadata document URL (optional)",
				kind: "text"
			},
			{
				name: "redirect-url",
				label: "Callback URL override (optional; must be routed to this environment or completed below)",
				kind: "text"
			}
		]
	});
	for (const account of accounts) if (account.pending && account.authorizationUrl) {
		blocks.push({
			id: `signin-${account.id}`,
			kind: "link",
			label: `Sign in to ${account.label} (${account.project ?? "all projects"})`.slice(0, 200),
			url: account.authorizationUrl
		});
		blocks.push({
			id: `callback-${account.id}`,
			kind: "form",
			label: "Complete sign-in from a remote browser",
			command: "oauth-complete",
			input: { id: account.id },
			fields: [{
				name: "callback",
				label: "Full callback URL (paste the address after signing in if the browser cannot reach the environment)",
				kind: "password",
				required: true
			}]
		});
	}
	return blocks;
}
// src/store.ts
const exec = promisify(execFile);
const emptyCredentials = () => ({
	environment: {},
	headers: {}
});
const ownedSecretKey = () => `server-${randomUUID().replaceAll("-", "")}`;
const isMissing = (error) => error instanceof Error && "code" in error && error.code === "ENOENT";
async function createStore(context) {
	await Fs.mkdir(context.dataDirectory, { recursive: true });
	const path = Path.join(context.dataDirectory, "servers.json");
	let state;
	try {
		state = decodeState(JSON.parse(await Fs.readFile(path, "utf8")));
	} catch (error) {
		if (!isMissing(error)) throw error;
		state = initialState();
	}
	let queue = Promise.resolve();
	const serial = (operation) => {
		const next = queue.then(operation);
		queue = next.catch(() => void 0);
		return next;
	};
	const persist = async (next) => {
		const temporary = `${path}.${randomUUID()}.tmp`;
		try {
			const file = await Fs.open(temporary, "wx", 384);
			try {
				await file.writeFile(JSON.stringify(next, null, 2) + "\n");
				await file.sync();
			} finally {
				await file.close();
			}
			await Fs.rename(temporary, path);
			state = next;
		} finally {
			await Fs.rm(temporary, { force: true });
		}
	};
	const credentials = async (server) => {
		const value = await runPromise(context.secrets.get(server.secretKey));
		if (value === null) throw Error("MCP credential entry is missing");
		const parsed = record(JSON.parse(value));
		return {
			environment: stringMap(parsed.environment),
			headers: stringMap(parsed.headers)
		};
	};
	const cleanup = async (key) => {
		try {
			await runPromise(context.secrets.remove(key));
		} catch {}
	};
	const find = (id) => {
		const server = state.servers.find((s) => s.id === id);
		if (!server) throw Error("Unknown MCP server");
		return server;
	};
	const update = (server) => persist({
		version: 2,
		servers: state.servers.map((s) => s.id === server.id ? server : s)
	});
	return {
		list: () => serial(async () => structuredClone(state)),
		get: (id) => serial(async () => {
			const server = state.servers.find((server) => server.id === id);
			return server ? structuredClone(server) : void 0;
		}),
		credentials: (server) => serial(() => credentials(server)),
		save: (input) => serial(async () => {
			const project = input.scope === "project" ? await Fs.realpath(directory(input.project)) : null;
			const name = text(input.name, "server name", 48);
			const previous = state.servers.find((s) => s.name === name && s.project === project);
			if (!previous && state.servers.length >= 40) throw Error("At most 40 server definitions are supported");
			const server = parseServer({
				...input,
				project
			}, previous?.id ?? randomUUID(), previous);
			const after = { ...previous ? await credentials(previous) : emptyCredentials() };
			for (const [field, target] of [["environment", "environment"], ["headers", "headers"]]) {
				const value = input[field];
				if (typeof value === "string" && value.trim()) after[target] = stringMap(JSON.parse(value));
				else if (value !== void 0 && typeof value !== "string") throw Error("Credentials must be JSON objects");
			}
			if (server.transport !== "stdio" && Object.entries(after.headers).some(([key, value]) => /[\r\n]/.test(key + value))) throw Error("Invalid HTTP header");
			server.secretKey = ownedSecretKey();
			await runPromise(context.secrets.set(server.secretKey, JSON.stringify(after)));
			try {
				await persist({
					version: 2,
					servers: previous ? state.servers.map((s) => s.id === previous.id ? server : s) : [...state.servers, server]
				});
			} catch (error) {
				await cleanup(server.secretKey);
				throw error;
			}
			if (previous) {
				await cleanup(previous.secretKey);
				if (previous.url !== server.url || previous.auth !== server.auth) await cleanup(`oauth-${server.id.replaceAll("-", "")}`);
			}
			return {
				id: server.id,
				saved: true
			};
		}),
		setEnabled: (id, enabled) => serial(async () => {
			if (typeof enabled !== "boolean") throw Error("Invalid enabled value");
			const server = find(id);
			await update({
				...server,
				enabled
			});
			return {
				id: server.id,
				enabled
			};
		}),
		setProvider: (id, provider, enabled) => serial(async () => {
			const key = text(provider, "provider", 128);
			if (!/^[a-zA-Z][a-zA-Z0-9_.:-]*$/.test(key) || typeof enabled !== "boolean") throw Error("Invalid provider rule");
			const server = find(id);
			await update({
				...server,
				providers: {
					...server.providers,
					[key]: enabled
				}
			});
			return {
				id: server.id,
				provider: key,
				enabled
			};
		}),
		remove: (id) => serial(async () => {
			const server = find(id);
			await persist({
				version: 2,
				servers: state.servers.filter((s) => s.id !== id)
			});
			await cleanup(server.secretKey);
			await cleanup(`oauth-${server.id.replaceAll("-", "")}`);
			return { removed: true };
		}),
		clearCredentials: (id) => serial(async () => {
			const server = find(id);
			const next = {
				...server,
				secretKey: ownedSecretKey()
			};
			await runPromise(context.secrets.set(next.secretKey, JSON.stringify(emptyCredentials())));
			try {
				await update(next);
			} catch (error) {
				await cleanup(next.secretKey);
				throw error;
			}
			await cleanup(server.secretKey);
			return { cleared: true };
		}),
		resolve: (launch, oauth) => serial(async () => {
			let project = launch.projectDirectory ? await Fs.realpath(launch.projectDirectory) : void 0;
			if (!project && launch.cwd) {
				const cwd = await Fs.realpath(launch.cwd);
				try {
					const result = await exec("git", [
						"-C",
						cwd,
						"rev-parse",
						"--path-format=absolute",
						"--git-common-dir"
					], {
						timeout: 2e3,
						maxBuffer: 16384,
						windowsHide: true
					});
					const common = await Fs.realpath(result.stdout.trim());
					project = Path.basename(common) === ".git" ? Path.dirname(common) : cwd;
				} catch {
					project = cwd;
				}
				project = state.servers.map((s) => s.project).filter((p) => p !== null && (cwd === p || cwd.startsWith(p + Path.sep))).sort((a, b) => b.length - a.length)[0] ?? project;
			}
			const result = {};
			for (const server of selectedServers(state, launch, project)) result[`mcp-manager-${server.name}`] = server.auth === "oauth" ? oauth ? await oauth(server) : (() => {
				throw Error("OAuth connection is not configured");
			})() : serverConfig(server, await credentials(server));
			return result;
		})
	};
}
// src/plugin.ts
const VERSION = "0.2.0";
const call = (operation, run) => tryPromise({
	try: run,
	catch: (cause) => new PluginError({
		operation,
		cause
	})
});
const action = (id, label, command, input) => ({
	id,
	label,
	command,
	input
});
const publicServer = (server) => ({
	id: server.id,
	name: server.name,
	label: server.label,
	project: server.project,
	enabled: server.enabled,
	providers: server.providers,
	transport: server.transport,
	auth: server.auth
});
function settings(state) {
	const fields = [
		{
			name: "name",
			label: "Server name (letters, numbers, hyphens)",
			kind: "text",
			required: true
		},
		{
			name: "label",
			label: "Display name",
			kind: "text"
		},
		{
			name: "scope",
			label: "Scope",
			kind: "select",
			value: "global",
			options: [{
				label: "All projects on this environment",
				value: "global"
			}, {
				label: "One project (including its worktrees)",
				value: "project"
			}]
		},
		{
			name: "project",
			label: "Absolute project directory (project scope only)",
			kind: "text"
		},
		{
			name: "transport",
			label: "Transport",
			kind: "select",
			value: "stdio",
			options: [
				{
					label: "Local command (stdio)",
					value: "stdio"
				},
				{
					label: "Streamable HTTP",
					value: "http"
				},
				{
					label: "Legacy SSE",
					value: "sse"
				}
			]
		},
		{
			name: "command",
			label: "Command (stdio)",
			kind: "text"
		},
		{
			name: "args",
			label: "Arguments as a JSON array",
			kind: "text",
			value: "[]"
		},
		{
			name: "url",
			label: "Server URL (HTTP or SSE)",
			kind: "text"
		},
		{
			name: "environment",
			label: "Secret environment variables as JSON (blank keeps existing)",
			kind: "password"
		},
		{
			name: "headers",
			label: "Secret HTTP headers as JSON (blank keeps existing)",
			kind: "password"
		},
		{
			name: "auth",
			label: "Authentication",
			kind: "select",
			value: "static",
			options: [{
				label: "Environment variables or HTTP headers",
				value: "static"
			}, {
				label: "OAuth (Streamable HTTP)",
				value: "oauth"
			}]
		},
		...PROVIDERS.map(([id, label]) => ({
			name: `provider-${id.toLowerCase()}`,
			label: `Enable for ${label}`,
			kind: "boolean",
			value: true
		}))
	];
	return {
		title: "MCP Manager",
		blocks: [
			{
				id: "scope",
				kind: "text",
				text: "Environment-wide defaults with project overrides. Saving the same name and scope updates that definition. A project definition replaces the global definition with the same name, including when disabled. Changes apply to new provider sessions; restart an existing session to refresh its servers."
			},
			{
				id: "support",
				kind: "text",
				text: "Claude and Cursor accept all three transports. Codex and Muse accept stdio and Streamable HTTP. ACP providers (Grok, Antigravity, Kiro and fx) require advertised transport support; stdio is the portable choice. OpenCode requires a T3-managed server. Pi is not supported. Provider switches also apply to future instances of that provider."
			},
			{
				id: "credentials",
				kind: "text",
				text: "Commands execute on the selected environment. Keep credentials in the secret fields, never in command arguments or URLs. Existing credentials are never displayed or prefilled. Removing the plugin stops applying its configuration and removes its managed settings; provider configuration files are never rewritten."
			},
			{
				id: "servers",
				kind: "table",
				columns: [
					"Name",
					"Scope",
					"Transport",
					"Authentication",
					"State",
					"Enabled providers"
				],
				rows: state.servers.map((server) => ({
					id: server.id,
					cells: [
						server.label,
						server.project ?? "All projects",
						server.transport,
						server.auth === "oauth" ? "OAuth" : "Static credentials",
						server.enabled ? "Enabled" : "Disabled",
						PROVIDERS.filter(([id]) => server.providers[id] !== false).map(([, label]) => label).join(", ") || "None"
					],
					actions: [
						action("toggle", server.enabled ? "Disable" : "Enable", "set-enabled", {
							id: server.id,
							enabled: !server.enabled
						}),
						...PROVIDERS.map(([id, label]) => action(`toggle-${id.toLowerCase()}`, `${server.providers[id] === false ? "Enable" : "Disable"} ${label}`, "set-provider", {
							id: server.id,
							provider: id,
							enabled: server.providers[id] === false
						})),
						action("clear-credentials", "Clear static credentials", "clear-credentials", { id: server.id }),
						action("remove", "Remove", "remove", { id: server.id })
					]
				}))
			},
			{
				id: "save",
				kind: "form",
				label: "Add or update an MCP server",
				command: "save",
				fields,
				input: {}
			},
			{
				id: "instance-rule",
				kind: "form",
				label: "Override one provider instance",
				command: "set-provider",
				fields: [
					{
						name: "id",
						label: "Server",
						kind: "select",
						options: state.servers.map((s) => ({
							label: `${s.label} (${s.project ?? "all projects"})`.slice(0, 200),
							value: s.id
						})),
						required: true
					},
					{
						name: "provider",
						label: "Provider instance ID",
						kind: "text",
						required: true
					},
					{
						name: "enabled",
						label: "Enabled",
						kind: "boolean",
						value: true
					}
				],
				input: {}
			}
		]
	};
}
async function activateManager(context) {
	if (!context.capabilities?.includes("provider-mcp-servers") || !context.capabilities?.includes("http-response-timeout") || !context.capabilities?.includes("settings-pages")) throw Error("This plugin requires the PoC host with provider MCP configuration and standalone settings pages");
	const store = await createStore(context);
	const changes = await runPromise(unbounded());
	const oauth = await createOAuth(context, store, { changed: () => runPromise(publish(changes, void 0)).then(() => void 0) });
	const descriptor = (id, title, run, read = false) => ({
		id,
		title,
		description: title,
		permission: read ? "orchestration:read" : "providers:manage",
		agent: read,
		run: (input) => call(id, () => run(input)).pipe(map((value) => value))
	});
	return {
		commands: [
			descriptor("list", "List managed MCP server policies without credentials", async () => ({ servers: (await store.list()).servers.map(publicServer) }), true),
			descriptor("save", "Save an MCP server definition and write-only credentials", (input) => store.save(input).then(async (result) => {
				await oauth.policyChanged(result.id);
				return result;
			})),
			descriptor("set-enabled", "Enable or disable an MCP server", (input) => store.setEnabled(input.id, input.enabled).then(async (result) => {
				await oauth.policyChanged(result.id);
				return result;
			})),
			descriptor("set-provider", "Enable or disable an MCP server for a provider or provider instance", (input) => store.setProvider(input.id, input.provider, input.enabled)),
			descriptor("clear-credentials", "Clear credentials for an MCP server", (input) => store.clearCredentials(input.id).then(async (result) => {
				await oauth.policyChanged(String(input.id));
				return result;
			})),
			descriptor("remove", "Remove an MCP server definition and its credentials", (input) => store.remove(input.id).then(async (result) => {
				await oauth.policyChanged(String(input.id));
				return result;
			})),
			descriptor("oauth-start", "Connect an independent OAuth account for a server and project", (input) => oauth.start(input)),
			descriptor("oauth-complete", "Complete this project's OAuth sign-in", (input) => oauth.finish(input)),
			descriptor("oauth-cancel", "Cancel this project's pending OAuth sign-in", (input) => oauth.cancel(String(input.id))),
			descriptor("oauth-disconnect", "Disconnect only this project's OAuth account", (input) => oauth.disconnect(String(input.id)))
		],
		views: [{
			id: "settings",
			title: "MCP Manager",
			location: "settings-page",
			permission: "providers:manage",
			read: () => call("settings", async () => {
				const view = settings(await store.list());
				const base = await runPromise(context.httpBaseUrl);
				return {
					...view,
					blocks: [...view.blocks, ...oauthBlocks(await oauth.status(), base.replace(/\/$/, "") + "/oauth/callback")]
				};
			})
		}],
		mcpServers: (context) => call("prepare-mcp", () => store.resolve(context, oauth.resolve)),
		httpResponseTimeoutMs: 39e5,
		handleHttp: (request) => call("http", () => oauth.handleHttp(request)),
		changes: fromPubSub(changes),
		async [Symbol.asyncDispose]() {
			await oauth.close();
			await runPromise(shutdown(changes));
		}
	};
}
var plugin_default = {
	apiVersion: 1,
	id: "mcp-manager",
	name: "MCP Manager",
	version: VERSION,
	description: "Manage MCP servers across projects and provider instances without editing provider configuration files.",
	activate: (context) => call("activate", () => activateManager(context)).pipe(tap((contributions) => addFinalizer(() => call("shutdown", () => contributions[Symbol.asyncDispose]()).pipe(catch_(() => void_)))))
};
export { activateManager, plugin_default as default };
