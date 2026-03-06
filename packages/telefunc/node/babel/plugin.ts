export default BabelPluginTelefunc

import { parse } from '@babel/parser'
import { transformTelefuncFileClientSideSync } from '../shared/transformer/transformTelefuncFileClientSideSync.js'
import { toPosixPath } from '../../utils/path.js'
import type { PluginObj, NodePath } from '@babel/core'
import type { types as BabelTypes } from '@babel/core'

function getExportsFromBabelAST(programNodePath: NodePath<BabelTypes.Program>, types: typeof BabelTypes) {
  const body = programNodePath.node.body

  const exported = []
  for (let index = 0; index < body.length; index++) {
    const subNode = body[index]

    // export default fnName
    if (types.isExportDefaultDeclaration(subNode)) {
      exported.push('default')
    }

    if (types.isExportNamedDeclaration(subNode)) {
      if (subNode.specifiers.length > 0) {
        // Handles cases:
        // export { functionName };
        // export { functionName as fnName };
        // export { functionName as "fnName" };
        // export { "fnName" } from "package";
        for (const specifier of subNode.specifiers) {
          if (specifier.exported.type === 'Identifier') {
            // export { functionName };
            // export { functionName as fnName };
            exported.push(specifier.exported.name)
          } else if (specifier.exported.type === 'StringLiteral') {
            // export { functionName as "fnName" };
            // export { "fnName" } from "package";
            exported.push(specifier.exported.value)
          }
        }
      } else if (types.isFunctionDeclaration(subNode.declaration)) {
        // export function fn() {}
        // export async function fn() {}
        exported.push(subNode.declaration.id!.name) // Function must have ID if it's part of a named export
      } else if (types.isVariableDeclaration(subNode.declaration)) {
        // export const fnName = () => {}
        // export var fnName = () => {}
        // export let fnName = () => {}
        // export const fnName = function() {}
        // export var fnName = function() {}
        // export let fnName = function() {}
        const declarator = subNode.declaration.declarations[0]!
        if (
          'name' in declarator.id &&
          (types.isFunctionExpression(declarator.init) || types.isArrowFunctionExpression(declarator.init))
        ) {
          exported.push(declarator.id.name) // Function must have ID if it's part of a named export
        }
      }
    }
  }

  return exported
}

function BabelPluginTelefunc(babel: { types: typeof BabelTypes }): PluginObj {
  return {
    visitor: {
      Program(path, context) {
        const filename: string = context.filename!

        if (!filename.includes('.telefunc.')) return
        if (isFileAlreadyTransformed(path, babel.types)) return

        const exportNames = getExportsFromBabelAST(path, babel.types)

        const root: string = context.file.opts.root!
        const transformed: string = transformTelefuncFileClientSideSync(
          toPosixPath(filename),
          toPosixPath(root),
          exportNames,
        ).code

        const parsed = parse(transformed, {
          sourceType: 'module',
        })

        path.replaceWith(parsed.program)
      },
    },
  }
}

function isFileAlreadyTransformed(path: NodePath<BabelTypes.Program>, types: typeof BabelTypes): boolean {
  return path.node.body.some((t) => {
    if (!types.isImportDeclaration(t)) return false
    if (t.specifiers.length === 0) return false

    const specifier = t.specifiers[0]
    if (!types.isImportSpecifier(specifier)) return false
    if (!types.isImportSpecifier(specifier)) return false
    if (!types.isIdentifier(specifier.imported)) return false

    return specifier.imported.name === '__remoteTelefunctionCall'
  })
}
