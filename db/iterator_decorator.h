#ifndef MERODIS_ITERATOR_DECORATOR_H
#define MERODIS_ITERATOR_DECORATOR_H

#include "merodis/merodis.h"

namespace merodis {

class IteratorDecorator : public Iterator {
public:
  IteratorDecorator(Iterator* iter): iter_(iter) {}
  IteratorDecorator(const IteratorDecorator&) = delete;
  IteratorDecorator& operator=(const IteratorDecorator&) = delete;
  ~IteratorDecorator() override { delete iter_; }

  bool Valid() const override { return iter_->Valid(); }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }
  void Seek(const Slice& target) override { iter_->Seek(target); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }
  Slice key() const override { return iter_->key(); }
  Slice value() const override { return iter_->value(); }
  Status status() const override { return iter_->status(); }

protected:
  Iterator* iter_;
};

}

#endif //MERODIS_ITERATOR_DECORATOR_H
