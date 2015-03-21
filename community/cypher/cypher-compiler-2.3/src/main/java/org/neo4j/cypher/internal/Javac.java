/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.SecureClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import static javax.tools.JavaCompiler.CompilationTask;

public class Javac
{
    @SuppressWarnings( "unchecked" )
    public static ExecutablePlan newInstance( String className, String classBody ) throws Exception
    {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        JavaFileManager manager = new InMemFileManager();
        DiagnosticCollector<JavaFileObject> diagnosticsCollector = new DiagnosticCollector<>();
        Iterable<? extends JavaFileObject> sources = Arrays.asList( new InMemSource( className, classBody ) );
        CompilationTask task = compiler.getTask( null, manager, diagnosticsCollector, null, null, sources );

        task.call();

        Class<?> clazz = manager.getClassLoader( null ).loadClass( "org.neo4j.cypher.internal." + className );
        return (ExecutablePlan) clazz.newInstance();
    }

    private static class InMemSource extends SimpleJavaFileObject
    {
        final String javaSource;

        InMemSource( String className, String javaSource )
        {
            super( URI.create( "string:///" + className.replace( '.', '/' ) + Kind.SOURCE.extension ), Kind.SOURCE );
            this.javaSource = javaSource;
        }

        @Override
        public CharSequence getCharContent( boolean ignoreEncodingErrors )
        {
            return javaSource;
        }
    }

    private static class InMemSink extends SimpleJavaFileObject
    {
        private ByteArrayOutputStream byteCodeStream = new ByteArrayOutputStream();

        InMemSink( String className )
        {
            super( URI.create( "mem:///" + className + Kind.CLASS.extension ), Kind.CLASS );
        }

        public byte[] getBytes()
        {
            return byteCodeStream.toByteArray();
        }

        @Override
        public OutputStream openOutputStream() throws IOException
        {
            return byteCodeStream;
        }
    }

    private static class InMemFileManager extends ForwardingJavaFileManager
    {
        private final Map<String,InMemSink> classNameToByteCode = new HashMap<>();

        InMemFileManager()
        {
            super( ToolProvider.getSystemJavaCompiler().getStandardFileManager( null, null, null ) );
        }

        @Override
        public ClassLoader getClassLoader( Location location )
        {
            return new SecureClassLoader()
            {
                @Override
                protected Class<?> findClass( String name ) throws ClassNotFoundException
                {
                    byte[] byteCode = classNameToByteCode.get( name ).getBytes();
                    return super.defineClass( name, byteCode, 0, byteCode.length );
                }
            };
        }

        @Override
        public JavaFileObject getJavaFileForOutput( Location location, String className,
                JavaFileObject.Kind kind, FileObject sibling ) throws IOException
        {
            if ( StandardLocation.CLASS_OUTPUT == location && JavaFileObject.Kind.CLASS == kind )
            {
                InMemSink clazz = new InMemSink( className );
                classNameToByteCode.put( className, clazz );
                return clazz;
            }
            else
            {
                return super.getJavaFileForOutput( location, className, kind, sibling );
            }
        }
    }
}
