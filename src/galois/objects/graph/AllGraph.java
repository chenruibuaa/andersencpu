package galois.objects.graph;

import galois.objects.GObject;
interface AllGraph<N extends GObject,E> extends ObjectGraph<N, E>, IntGraph<N>,
    LongGraph<N>, FloatGraph<N>, DoubleGraph<N> {
}
