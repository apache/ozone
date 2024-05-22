/**
 * Since Jest would not have access to the browser session storage as it is a virtual dom
 * we need to implement out session storage as a mock, defining the various session storage
 * functions to be used with the virtual DOM
 */

const localStorageMock = (function () {
    let store: { [key: string]: any } = {}

    return {
        getItem: function (key: string): any | null {
            return store[key] || null;
        },
        setItem: function (key: string, value: any): void {
            store[key] = value.toString();
        },
        removeItem: function (key: string): void {
            delete store[key]
        },
        clear: function (): void {
            store = {}
        }
    }
});

Object.defineProperty(window, 'localStorage', {
    value: localStorageMock
});
