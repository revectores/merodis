#ifndef MERODIS_VARIANT_HELPER_H

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

#define MERODIS_VARIANT_HELPER_H

#endif //MERODIS_VARIANT_HELPER_H
