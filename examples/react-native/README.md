Example of using Telefunc with [React Native](https://reactnative.dev/) and [Expo](https://expo.dev/).

To run it:
```bash
git clone git@github.com:telefunc/telefunc
cd telefunc/examples/react-native/
npm install
npm run start
```

To build and use Telefunc from source code:
```bash
npm run build:telefunc
```

> :information_source: Symlinks do not work with Metro; that's why we cannot use `npm link` and we use `package.json#scripts.build:telefunc` instead.
